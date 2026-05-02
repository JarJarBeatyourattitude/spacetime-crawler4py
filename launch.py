from argparse import ArgumentParser
from configparser import ConfigParser
import multiprocessing
import sys


def configure_multiprocessing():
    # spacetime creates a local Process subclass that is not picklable under
    # macOS's default "spawn" start method. Using "fork" matches the older
    # course setup and avoids registration-time crashes.
    if sys.platform != "darwin":
        return

    if multiprocessing.get_start_method(allow_none=True) is None:
        multiprocessing.set_start_method("fork")


def main(config_file, restart):
    configure_multiprocessing()

    from utils.server_registration import get_cache_server
    from utils.config import Config
    from crawler import Crawler

    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
