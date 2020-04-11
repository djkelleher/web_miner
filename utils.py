import aioredis
from urllib.parse import urlsplit, urlunsplit
from typing import Optional, Union, Generator
from pathlib import Path
import asyncio
import logging
import logging.handlers
import re
import sys

url_re = re.compile(r'<a[^<>]+?href=([\'\"])(.*?)\1', re.IGNORECASE)

error_res = [
    re.compile(er) for er in
    (r"(?i)(not|aren't).{1,10}robot", r"(?i)(click|select|check).{1,20}box",
     r"(?i)(verify|check|confirm).{1,40}human",
     r"(?i)(enter|type).{1,20}characters",
     r"(?i)(select|click|choose).{1,30}(image|picture)",
     r"(?i)(not|don't|can't).{1,20}(access|permission|permitted).{1,20}server",
     r"(?i)access.{1,20}denied", r"(?i)browser.{1,50}(cookies|javascript)",
     r"(?i)something.{1,20}(isn't|is\snot).{1,20}(right|normal)",
     r"(?i)(traffic|activity).{1,50}(unusual|suspicious)",
     r"(?i)(unusual|suspicious).{1,50}(traffic|activity)",
     r"(?i)dns.{1,30}(failed|error)", r"(?i)error.{1,10}not\sfound",
     r"(?i)retriev.{1,5}the url",
     r"(?i)ip\saddress.{1,20}(banned|blocked|permitted)",
     r"(?i)automated\saccess")
]


def permission_error(text: str):
    text_size = sys.getsizeof(text)
    if text_size < 6000:
        match_count = len(
            [m for m in error_res if re.search(m, text) is not None])
        if match_count > 1:
            return True
    return False


def get_logger(logger_name: str,
               log_save_path: Optional[Union[str, Path]] = None,
               log_level: int = logging.INFO):
    logging.basicConfig(
        format='[%(name)s][%(levelname)s][%(asctime)s] %(message)s',
        level=log_level)
    logger = logging.getLogger(logger_name)
    if log_save_path is not None:
        log_save_path = Path(log_save_path)
        if not log_save_path.parent.is_dir():
            try:
                log_save_path.parent.mkdir(exist_ok=True, parents=True)
            except (FileNotFoundError, PermissionError) as e:
                logger.error(
                    f"Error creating log directory '{log_save_path.parent}'. No log will be saved. Error: {e}"
                )
                return logger
        logger.info(f"Using log_save_path '{log_save_path}'")
        fh = logging.handlers.RotatingFileHandler(log_save_path,
                                                  maxBytes=10_000_000,
                                                  backupCount=2)
        logger.addHandler(fh)
    return logger


def make_absolute_url(check_url: str, page_url: str):
    # If url is relative, make it absolute.
    check_url_split = urlsplit(check_url)
    if check_url_split.scheme == "" or check_url_split.netloc == "":
        page_url_split = urlsplit(page_url)
        check_url = urlunsplit((page_url_split.scheme, page_url_split.netloc,
                                check_url_split.path, check_url_split.query,
                                check_url_split.fragment))
    return check_url


def extract_urls(page_html: str, page_url: str):
    urls = set([match.group(2) for match in url_re.finditer(page_html)])
    return [make_absolute_url(url, page_url) for url in urls]


class CyclicPriorityQueue:
    def __init__(self, redis_client: str, key_namespace: Optional[str]):
        self.redis_client = redis_client
        # useing a namespace enables queue persistance if script is stopped and started again.
        self.key_namespace = key_namespace if key_namespace else ""

    async def init(self):
        self.key_iter = self.key_iter__()
        return self

    async def put(self, url: str, url_depth: int, hash_key: str) -> None:
        # generate key using url's domain so we can cycle over domains.
        key = self.key_namespace + urlsplit(url).netloc
        await self.redis_client.zadd(key, url_depth, hash_key)

    async def get(self) -> str:
        key = await self.key_iter.__anext__()
        key_hash_depth = await self.redis_client.bzpopmin(
            key, encoding='utf-8'
        )  # min scrore should be url with smallest depth from start url.
        return key_hash_depth[1]

    # cycle through domains (to evenly distribute request load) and sort urls based on depth from start url.
    async def key_iter__(self) -> Generator[str, None, None]:
        #cur, match = b'0', self.key_namespace + '*'
        while True:
            #cur, keys = await self.redis_client.scan(cur, match=self.key_namespace+'*')
            match = self.key_namespace + '*'
            match_keys = await self.redis_client.keys(pattern=match)
            if match_keys:
                #async for key in redis_client.iscan(match=self.key_namespace+'*'):
                for key in match_keys:
                    yield key
            else:
                logging.info(f"Waiting for keys matching {match}")
                await asyncio.sleep(1)
