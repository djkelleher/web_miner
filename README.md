This script is intended to be used a command line tool (see parse_args function for description of arguments).   
All queueing and data storage is done via Redis, so by default multiple script instances can run simultaneously and work on the same data.