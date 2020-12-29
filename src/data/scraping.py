"""
This module contain scrapers that can extract data from the government website.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import bs4 as bs
import requests
from dateutil.parser import parse

from src.utils import pickle_obj


class Scraper:
    """
    Scraper base class
    """

    def __init__(self, government_n=9, local_backup=False):
        """
        Creates instance of a Scraper
        Args:
            government_n (int): which government cadence should be scraped.
                                Default is 9, which is cadence that ruled since 2019 to 2023
        """
        self.government_n = government_n
        self.root_url = fr'https://www.sejm.gov.pl/sejm{self.government_n}.nsf/'
        self.local_backup = local_backup


class SpeechesScraper(Scraper):
    """
    Scraper that crawl through all politician speeches urls and extracts text from stenograms.
    """
    def __init__(self, government_n, local_backup):
        super().__init__(government_n, local_backup)

        self.speeches_url = self.root_url + r'/wypowiedzi.xsp'

        # Namespace
        self.speeches = []

    def scrape_politician_speeches(self):
        """
        Scraping all speeches and adding them to the speeches attribute
        """

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._scrape_all_speeches,
                                       id=politician_id, name=politician_name, suffix=politician_suffix)
                       for politician_id, politician_name, politician_suffix
                       in self._speeches_per_politician_url(self.speeches_url + '?view=3')]

        for thread in as_completed(futures):
            self.speeches += thread.result()

        if self.local_backup:
            pickle_obj(self.speeches, "speeches.pickle")

        # for politician_id, politician_name, politician_suffix in self._speeches_per_politician_url(self.speeches_url + '?view=3'):
        #     self.speeches += self._scrape_all_speeches(id=politician_id, name=politician_name, suffix=politician_suffix)

    def _scrape_all_speeches(self, id, name, suffix):
        speeches = []
        for speech_url in self._iterate_through_speeches_in_politician_url(self.speeches_url + suffix):
            speeches.append(
                (id, name, self._extract_text_from_speech(self.root_url + speech_url))
            )
        return speeches

    @staticmethod
    def _extract_text_from_speech(url):
        """

        Args:
            url:

        Returns:

        """

        soup = bs.BeautifulSoup(requests.get(url).content, features="html.parser")

        # Cleaning function that will be used will looping through parts of speech
        cleen_text = lambda speech_part: speech_part.get_text(strip='\xa0').replace("\r\n", "")

        speech = " ".join(
            [cleen_text(speech_part)
             for speech_part in soup.find("div", {"class": "stenogram"}).findAll("p")[1:]])

        return speech

    def _iterate_through_speeches_in_politician_url(self, url):
        soup = bs.BeautifulSoup(requests.get(url).content, features="html.parser")
        print(url)
        pages = []
        if not (page_navigation := soup.findAll("ul", {"class": "pagination"})):
            pages.append(url)
        else:
            for page_tag in page_navigation[0].findAll("li")[1:-1]:
                pages.append(self.speeches_url + page_tag.findChild().get_attribute_list("href")[0].replace(" ", '%20'))

        for page_url in pages:
            soup = bs.BeautifulSoup(requests.get(page_url).content, features="html.parser")
            table = soup.find('table', {'class': "table border-bottom lista-wyp"})

            for row in table.findAll("tr"):
                try:
                    yield row.findAll("td")[-2].find("a").get_attribute_list("href")[0]
                except IndexError:
                    continue

    @staticmethod
    def _speeches_per_politician_url(url):
        soup = bs.BeautifulSoup(requests.get(url).content, features="html.parser")
        for politician in soup.find('ul', {'class': "category-list"}).find_all("li"):
            for tag in politician:
                if link := tag.get_attribute_list("href")[0]:
                    politician_id = link[-3:]
                    politician_name = tag.get_text()
                    yield politician_id, politician_name, link


class PoliticiansScraper(Scraper):

    def __init__(self, government_n, local_backup):
        super().__init__(government_n, local_backup)

        self.politicians = []

    def scrape_politicians(self):

        padding = lambda n: str(n).rjust(3, "0")

        last_politician_number = self._find_last_politician_number()
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._scrape_single_politician,
                                       url=self.root_url + rf'posel.xsp?id={padding(politician_number)}&type=A')
                       for politician_number in range(1, last_politician_number + 1)]

        self.politicians += [thread.result() for thread in as_completed(futures)]

        # Scrape additional politicians that doesn't exist on the website
        while True:
            try:
                self.politicians.append(
                    self._scrape_single_politician(
                        self.root_url + rf'posel.xsp?id={padding(int(last_politician_number) + 1)}&type=A'))
            except StopIteration:
                break
            last_politician_number += 1

        if self.local_backup:
            pickle_obj(self.politicians, "politicians.pickle")

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

        transform = {
            'election_date': lambda value: parse(value),
            'election_area': parse_election_area,
            'oath_date': lambda value: parse(value),
            'parliment_member': lambda value: re.findall(r'([XVI]+)', value).extend("IX") or [],
            'place_and_date_of_brith': parse_place_and_date_of_birth,
            'email': assemble_email
        }

        for feature, value in list(politician_dict.items()):  # We are changing the dictionary while looping
            try:
                politician_dict[feature] = transform[feature](value)
            except KeyError:
                continue

        del politician_dict['place_and_date_of_brith']

        return politician_dict


if __name__ == '__main__':
    # ps = PoliticiansScraper(government_n=9, local_backup=True)
    # ps.scrape_politicians()
    # print(len(ps.politicians))
    ss = SpeechesScraper(government_n=9, local_backup=True)
    ss.scrape_politician_speeches()
    print(len(ss.speeches))
