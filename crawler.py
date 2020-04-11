from utils import CyclicPriorityQueue, get_logger, extract_urls

from typing import List, Optional
from collections import defaultdict
from datetime import datetime
from pprint import pformat
from uuid import uuid1
import logging
import argparse
import uvloop
import signal
import aioredis
import asyncio
import aiohttp
import sys


class Crawler:
    def __init__(self, complete_hash_list: str, timeout: int, debug: bool,
                 redis_addr: str, queue_key_ns: Optional[str],
                 start_urls_key: Optional[str], max_depth: Optional[int]):
        log_level = logging.DEBUG if debug else logging.INFO
        self.logger = get_logger("Crawler", log_level=log_level)
        if sys.version_info < (3, 7):
            self.logger.error(
                f"Python version >= 3.7 is required. Detected version: {sys.version_info}. Exiting."
            )
            sys.exit(1)
        self.complete_hash_list = complete_hash_list
        self.timeout = int(timeout)
        self.debug = bool(debug)
        self.redis_addr = redis_addr
        self.queue_key_ns = queue_key_ns
        self.start_urls_key = start_urls_key
        self.max_depth = int(max_depth)

        self.req_count = 0
        self.exceptions = defaultdict(int)
        self.status_codes = defaultdict(int)

        self.loop = uvloop.new_event_loop()

        for s in (signal.SIGTERM, signal.SIGINT):
            self.loop.add_signal_handler(
                s, lambda s=s: self.loop.create_task(self.shutdown(s)))

        asyncio.set_event_loop(self.loop)
        self.loop.set_debug(bool(debug))
        self.loop.set_exception_handler(self.default_exception_handle)

        self.loop.create_task(self.launch())
        self.loop.run_forever()

    async def launch(self):
        self.logger.info("Launching crawler.")
        self.http_client = aiohttp.ClientSession(
            loop=self.loop,
            timeout=aiohttp.ClientTimeout(self.timeout),
            connector=aiohttp.TCPConnector(limit=1_000_000,
                                           limit_per_host=100,
                                           ssl=False))
        self.redis_client = await aioredis.create_redis(self.redis_addr,
                                                        loop=self.loop)
        self.hash_key_queue = await CyclicPriorityQueue(
            redis_client=self.redis_client,
            key_namespace=self.queue_key_ns).init()

        if self.start_urls_key:
            self.logger.info(
                f"Processing start urls (key: {self.start_urls_key})")
            async for url in self.redis_client.isscan(self.start_urls_key,
                                                      match='*'):
                self.loop.create_task(
                    self.process_response(
                        self.http_client.get(url.decode('utf-8'))))
            self.logger.info("Finished processing start urls.")
        self.loop.create_task(self.process_next_page_hash())

    async def process_next_page_hash(self):
        # wait for next page hash key.
        try:
            # get hash for lowest depth url of next domain.
            hash_key = await asyncio.wait_for(self.hash_key_queue.get(),
                                              timeout=(self.timeout / 1000) *
                                              2)
        except asyncio.TimeoutError:
            self.logger.info("No more page hash keys. Crawling finished.")
            return
        self.logger.debug(f"Processing hash key: {hash_key}")
        url = await self.redis_client.hget(hash_key, 'url', encoding='utf-8')
        self.logger.info(f"Fetching page: {url}")
        self.loop.create_task(
            self.process_response(self.http_client.get(url), hash_key))
        # use a short sleep time to allow other tasks to be executed.
        await asyncio.sleep(0.05)
        self.loop.create_task(self.process_next_page_hash())

    async def process_response(self, fetch_future, hash_key=None):
        # create key for new hash if we're not working with an initialized hash.
        if hash_key is None:
            hash_key = str(uuid1())
            current_depth = 0
        else:
            # get depth of the page we're processing.
            current_depth = await self.redis_client.hget(hash_key, 'depth')
        resp = await fetch_future
        html = await resp.text()
        url, status = str(resp.url), str(resp.status)
        self.logger.info(f"[{status}] {url}")
        self.status_codes[status] += 1
        # add data from response to hash.
        await self.redis_client.hmset_dict(
            hash_key, {
                'url': url,
                'html': html,
                'scrape_time': datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                'resp_status': status
            })
        # add hash key to completed hash list.
        await self.redis_client.lpush(self.complete_hash_list, hash_key)
        # depth for urls we're extracting from the page.
        new_depth = int(current_depth) + 1
        if self.max_depth:
            # check if we've reached max allowable depth.
            if new_depth > self.max_depth:
                self.logger.info(
                    f"Reached max depth ({self.max_depth}) extracted page urls will not be queued."
                )
                return
        extracted_urls = extract_urls(html, url)
        self.logger.info(
            f"Extracted {len(extracted_urls)} urls (depth {new_depth}) from {url}"
        )
        # initialize hashes for urls extracted from page.
        for new_url in extracted_urls:
            hash_key = str(uuid1())
            await self.redis_client.hmset_dict(hash_key, {
                'url': new_url,
                'referrer': url,
                'depth': new_depth
            })
            # add hash key to queue so it will be processed by process_next_page_hash
            await self.hash_key_queue.put(new_url, new_depth, hash_key)
        self.logger.info(f"Finished caching {len(extracted_urls)} urls")
        self.req_count += 1
        if self.req_count % 1000 == 0:
            self.logger.info(f"""Processed {self.req_count} pages.
                            Status codes: {self.status_codes}
                            Exceptions: {self.exceptions}""")

    def default_exception_handle(self, loop, context):
        self.exceptions[str(context.get('exception'))] += 1
        self.logger.info(f"Event loop caught exception: {pformat(context)}")

    async def shutdown(self, signame=None):
        self.logger.info(f"Caught signal {signame}. Shutting down.")
        self.redis_client.save()
        self.redis_client.close()
        await self.http_client.close()
        self.loop.stop()
        self.logger.info("Shutdown complete.")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--max_depth',
                        type=str,
                        help='Max allowed link depth from start url.')
    parser.add_argument('-s',
                        '--start_urls_key',
                        type=str,
                        help='Key to Redis set containing start urls.')
    parser.add_argument(
        '-l',
        '--complete_hash_list',
        type=str,
        default='complete_page_hashes',
        help=
        'Key to Redis list where hash keys for scraped page hashes will be stored.'
    )
    parser.add_argument('-a',
                        '--redis_addr',
                        type=str,
                        default='redis://localhost:6379/15',
                        help='Address of Redis server.')
    parser.add_argument('-t',
                        '--timeout',
                        type=int,
                        default=30_000,
                        help='Max time to wait for page to load (ms).')
    parser.add_argument(
        '-q',
        '--queue_key_ns',
        type=str,
        default='crawl_page_hash::',
        help='Namespace that queue will internally use for domain url sets.')
    parser.add_argument('-dbg',
                        '--debug',
                        type=bool,
                        default=False,
                        help='Run event loop in debug mode.')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    Crawler(complete_hash_list=args.complete_hash_list,
            timeout=args.timeout,
            debug=args.debug,
            redis_addr=args.redis_addr,
            queue_key_ns=args.queue_key_ns,
            start_urls_key=args.start_urls_key,
            max_depth=args.max_depth)
