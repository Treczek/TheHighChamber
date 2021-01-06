import argparse
import logging
from logging import config
from src.scraping.scraping import PoliticiansScraper, SpeechesScraper
import src.mongo.mongo_setup as mongo_setup
from ast import literal_eval
from src.utils import get_project_structure

STRUCTURE = get_project_structure()


def main(command_line=None):
    # Load logging configuration
    logging.config.fileConfig(STRUCTURE['root'].joinpath("logging.ini"),
                              defaults={'logfilename': str(STRUCTURE["log_folder"])})

    # Connecting to database
    mongo_setup.global_init()

    parser = argparse.ArgumentParser(command_line)
    subparsers = parser.add_subparsers()

    scrape_parser = subparsers.add_parser("scrape", help="Scraping data from the government website."
                                                         "Implemented scrapers: speeches, politicians")

    scrape_parser.add_argument('action', help="politicians / speeches / all")
    scrape_parser.add_argument('-s', '--scraper_arg', required=False, nargs=2, action='append',
                               help='argument for the scraper class. You can change the government cadence number '
                                    '[government_n], and local_backup boolean flag')

    scrape_parser.set_defaults(which="scrape")

    for subparser in [scrape_parser]:
        getattr(subparser, 'add_argument')('-l', '--logging',
                                           choices=['debug', 'info', 'warning', 'error', 'critical'],
                                           default='info',
                                           help="Specify main logger logging level on StreamHandler")
    args = parser.parse_args(command_line)

    # Updating main logger logging level
    log_level = getattr(logging, args.logging.upper())
    main_log = logging.getLogger("main")
    for handler in main_log.handlers:
        if type(handler) == logging.StreamHandler:
            handler.setLevel(log_level)
    main_log.setLevel(log_level)

    if args.which == "scrape":

        scraper_args = {'government_n': 9,
                        'to_database': True,
                        'name_filter': None,
                        'only_new': True}

        if args.scraper_arg:
            passed_scraper_args = {arg: literal_eval(value) for arg, value in args.scraper_arg}
            scraper_args.update(passed_scraper_args)

        if args.action == "politicians" or args.action == "all":
            ps = PoliticiansScraper(**scraper_args)
            ps.scrape_politicians()

        if args.action == "speeches" or args.action == "all":
            ss = SpeechesScraper(**scraper_args)
            ss.scrape_politician_speeches()


if __name__ == '__main__':
    main()
