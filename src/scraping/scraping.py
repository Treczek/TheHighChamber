"""
This module contain scrapers that can extract data from the government website.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import bs4 as bs
import requests
from dateutil.parser import parse

from src.mongo.schemas import Politician
import src.mongo.utils as dbutils
from src.utils import get_project_structure, swap_name_with_surname

STRUCTURE = get_project_structure()


class Scraper:
    """
    Scraper base class
    """

    def __init__(self, government_n, to_database):
        """
        Creates instance of a Scraper
        Args:
            government_n (int): which government cadence should be scraped.
                                Default is 9, which is cadence that ruled since 2019 to 2023
        """

        self.main_log = logging.getLogger("main")
        self.mongo_log = logging.getLogger("main.mongo")

        self.government_n = government_n
        self.root_url = fr'https://www.sejm.gov.pl/sejm{self.government_n}.nsf/'
        self.to_database = to_database


class SpeechesScraper(Scraper):
    """
    Scraper that crawl through all politician speeches urls and extracts text from stenograms.
    """
    def __init__(self, government_n, to_database, only_new=True, name_filter=None):
        super().__init__(government_n, to_database)

        self.only_new = only_new
        self.speeches_url = self.root_url + r'/wypowiedzi.xsp'
        self.name_filter = name_filter

        if only_new:
            setattr(self, "last_speech", dbutils.get_last_speech_per_politician())

        # Namespace
        self.speeches = []

    def scrape_politician_speeches(self):
        """
        Scraping all speeches and adding them to the speeches attribute
        """

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._scrape_all_speeches, name=politician_name, suffix=politician_suffix)
                       for politician_name, politician_suffix
                       in self._speeches_per_politician_url(self.speeches_url + '?view=3')]

        for thread in as_completed(futures):
            self.speeches += thread.result()

    def _scrape_all_speeches(self, name, suffix):

        speeches = []
        for speech_date, speech_url in self._iterate_through_speeches_in_politician_url(self.speeches_url + suffix):
            if self.only_new and speech_date <= self.last_speech.get(name, datetime(1990, 12, 4)):
                continue
            try:
                speeches.append(
                    (name, speech_date, self._extract_text_from_speech(self.root_url + speech_url)))
            except AttributeError:
                continue

        if self.to_database:
            speeches_objs = [dbutils.create_speech_object(*speech) for speech in speeches]
            insert_results = [dbutils.insert_speech_into_db(speech) for speech in speeches_objs]
            self.mongo_log.info(f"Scraped {len(insert_results)} speeches of {name}. "
                                f"Inserted into db: {sum(insert_results)}. "
                                f"Duplicates: {len(insert_results) - sum(insert_results)}")
        else:
            self.main_log.info(f"Scraped {len(speeches)} speeches of {name}.")

        return speeches

    @staticmethod
    def _extract_text_from_speech(url):
        soup = bs.BeautifulSoup(requests.get(url).content, features="html.parser")

        # Cleaning function that will be used will looping through parts of speech
        cleen_text = lambda speech_part: speech_part.get_text(strip='\xa0').replace("\r\n", "")
        try:
            speech = " ".join(
                [cleen_text(speech_part)
                 for speech_part in soup.find("div", {"class": "stenogram"}).findAll("p")[1:]])
        except AttributeError:
            logging.getLogger("main").error(f"Błąd na stronie: {url}.")
            raise
        return speech

    def _iterate_through_speeches_in_politician_url(self, url):
        soup = bs.BeautifulSoup(requests.get(url).content, features="html.parser")
        pages = []
        if not (page_navigation := soup.findAll("ul", {"class": "pagination"})):
            pages.append(url)
        else:
            for page_tag in page_navigation[0].findAll("li")[1:-1]:
                pages.append(self.speeches_url + page_tag.findChild().get_attribute_list("href")[0].replace(" ", '%20'))

        for page_url in pages:
            soup = bs.BeautifulSoup(requests.get(page_url).content, features="html.parser")
            table = soup.find('table', {'class': "table border-bottom lista-wyp"})

            for row in table.findAll("tr")[1:]:
                try:
                    speech_date = row.find("td", {"class": "nobr"}).get_text()
                    speech_url = row.findAll("td")[-2].find("a").get_attribute_list("href")[0]
                    yield parse(speech_date), speech_url
                except IndexError:
                    continue

    def _speeches_per_politician_url(self, url):
        soup = bs.BeautifulSoup(requests.get(url).content, features="html.parser")
        for politician in soup.find('ul', {'class': "category-list"}).find_all("li"):
            for tag in politician:
                if link := tag.get_attribute_list("href")[0]:
                    politician_name = swap_name_with_surname(tag.get_text())
                    if not self.name_filter or self.name_filter in (politician_name, swap_name_with_surname(politician_name)):
                        yield politician_name, link


class PoliticiansScraper(Scraper):

    def __init__(self, government_n, to_database):
        super().__init__(government_n, to_database)

        self.politicians = []

    def scrape_politicians(self):

        padding = lambda n: str(n).rjust(3, "0")

        last_politician_number = self._find_last_politician_number()
        self.main_log.info(f"Found {last_politician_number} politicians on the website. Scraping started...")

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._scrape_single_politician,
                                       url=self.root_url + rf'posel.xsp?id={padding(politician_number)}&type=A')
                       for politician_number in range(1, last_politician_number + 1)]

        self.politicians += [thread.result() for thread in as_completed(futures)]
        self.main_log.info(f"Scraping finished. Looking for additional politicians...")

        # Scrape additional politicians that doesn't exist on the website
        while True:
            try:
                self.politicians.append(
                    self._scrape_single_politician(
                        self.root_url + rf'posel.xsp?id={padding(int(last_politician_number) + 1)}&type=A'))
            except StopIteration:
                break
            self.main_log.info(f"Found additional hidden politician: {self.politicians[-1]['name']}")
            last_politician_number += 1



        if self.to_database:
            self.politicians = [dbutils.create_politician_object(politician) for politician in self.politicians]
            Politician.objects.insert(self.politicians)
            self.mongo_log.info(f"{len(self.politicians)} politicians inserted to database")

    def _find_last_politician_number(self):
        web = requests.get(self.root_url + 'poslowie.xsp?type=A')
        soup = bs.BeautifulSoup(web.content, 'html.parser')

        all_links = [line.get_attribute_list("href") for line in soup.findAll("a")]

        pattern = re.compile(r"posel\.xsp\?id=(?P<number>\d{3})")
        for link in reversed(all_links):
            if match := pattern.search(link[0]):
                last_number = int(match.groupdict()['number'])
                break
        return last_number

    def _scrape_single_politician(self, url):
        politician_info = dict()
        web = requests.get(url)
        soup = bs.BeautifulSoup(web.content, 'html.parser')

        politician_info["name"] = soup.find(id="title_content").find("h1").get_text()

        if not politician_info["name"]:
            raise StopIteration("URL doesn't contain any politician data.")

        self.main_log.debug(f"Scraping politician: {url[-10:-7]} - {politician_info['name']}")

        features = list()
        values = list()

        for section in ["partia", "cv"]:
            for field in soup.find("div", {"class": section}).find("ul", {'class': "data"}):
                features.append(field.find("p", {"class": "left"}).get_text()[:-1])  # Last sign is a ":"
                values.append(field.find("p", {"class": "right"}).get_text())

        feature_map = {
            'Wybrany dnia': "election_date",
            'Lista': "election_list",
            'Okręg wyborczy': "election_area",
            'Liczba głosów': "number_of_votes",
            'Ślubowanie': "oath_date",
            'Staż parlamentarny': "parliment_member",
            'Klub/koło': "political_group",
            'Data i miejsce urodzenia': "place_and_date_of_brith",
            'Wykształcenie': 'education',
            'Ukończona szkoła': 'school',
            'Zawód': 'profession',
            'Wygaśnięcie mandatu': 'resign_date',
            'Wybrana dnia': "election_date",
            'Tytuł/stopień naukowy': 'academic_degree'}

        features = map(lambda feature: feature_map.get(feature, feature), features)

        politician_info.update(**dict(zip(features, values)))

        try:
            politician_info["email"] = soup.find(id="PoselEmail").next_sibling()[0]["href"]
        except AttributeError:  # email is not available
            pass

        return self._clean_politician_data(politician_info)

    @staticmethod
    def _clean_politician_data(politician_dict):

        def parse_election_area(value):
            nonlocal politician_dict

            election_area_n, election_area = value.split("\xa0\xa0")
            politician_dict['election_area_n'] = election_area_n
            return election_area

        def parse_place_and_date_of_birth(value):
            nonlocal politician_dict

            date_of_birth, place_of_birth = value.split(", ")
            politician_dict['place_of_birth'] = place_of_birth
            politician_dict['date_of_birth'] = parse(date_of_birth)
            politician_dict['age'] = (datetime.now() - politician_dict['date_of_birth']).days // 365

        def assemble_email(value):

            replacements = (
                ("#", ""),
                (" A T ", "@"),
                (" D O T ", "."),
                (" ", "")
            )
            for replacement in replacements:
                value = value.replace(*replacement)

            return value.lower()

        def extract_sex_from_name(value):
            politician_dict['sex'] = "woman" if value.split(" ")[0].endswith("a") else "man"
            return value

        def parse_parliment_member(value):
            previous_parliments = re.findall(r'([XVI]+)', value) or []
            return list(set(previous_parliments + ["IX"]))

        transform = {
            'election_date': lambda value: parse(value),
            'election_area': parse_election_area,
            'oath_date': lambda value: parse(value),
            'parliment_member': parse_parliment_member,
            'place_and_date_of_brith': parse_place_and_date_of_birth,
            'email': assemble_email,
            'name': extract_sex_from_name
        }

        for feature, value in list(politician_dict.items()):  # We are changing the dictionary while looping
            try:
                politician_dict[feature] = transform[feature](value)
            except KeyError:
                continue

        del politician_dict['place_and_date_of_brith']
        if "@" not in politician_dict.get('email', "@"):
            del politician_dict["email"]

        return politician_dict


if __name__ == '__main__':
    from main import main

    main(["scrape", "speeches", '-l', 'debug', '-s', 'to_database', 'False', '-s', 'only_new', 'True'])
