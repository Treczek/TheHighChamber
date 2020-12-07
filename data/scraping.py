import requests
import bs4 as bs
import re
import tempfile
from pathlib import Path
import pdftotext
from dateutil.parser import parse


def get_all_pdf_links_from_url(url):
    web = requests.get(url)
    soup = bs.BeautifulSoup(web.content, "lxml")
    all_links = [link.get('href') for link in soup.findAll('a')]
    return [link for link in all_links if link[-3:] == "pdf"]


def extract_text_from_pdf(pdf_url):
    response = requests.get(pdf_url)

    with tempfile.NamedTemporaryFile(suffix='.pdf', prefix="temp", delete=False) as file:
        Path(file.name).write_bytes(response.content)
        with open(file.name, "rb") as pdf:
            pdf_text = pdftotext.PDF(pdf)

    return "\n\n".join(pdf_text)


def parse_single_transcript_page(page_text):
    pattern = re.compile(r'\s?\s?(?P<LEFT>.*?)\s{3}\s?(?P<RIGHT>.*)')

    left = list()
    right = list()

    omit_lines = ['KANCELARIA SEJMU: redakcja i skład – Sekretariat Posiedzeń Sejmu',
                  'Informacja dla Sejmu i Senatu RP o udziale RP w pracach',
                  '. . . . . . .',
                  'TŁOCZONO Z POLECENIA MARSZAŁKA SEJMU',
                  'PL ISSN',
                  'Sprawozdanie Stenograficzne',
                  ]

    omit_regex = [r'z \d\d?. posiedzenia Sejmu Rzeczypospolitej Polskiej',
                  r'posiedzenie Sejmu w dniu \d\d? listopada \d{4} r.',
                  r'\s\s+\d+\s\s+']

    for line in page_text.split("\r\n"):
        if any([omit_line in line for omit_line in omit_lines]) or any(
                [re.search(pattern, line) for pattern in omit_regex]):
            continue
        elif line[:5] == " " * 5:
            right.append(line.lstrip(" "))
        elif len(line) < 65:
            left.append(line.strip(" "))
        else:
            try:
                left_part, right_part = pattern.search(line).groupdict().values()
                left.append(left_part.strip(" "))
                right.append(right_part.strip(" "))
            except AttributeError:
                print('ParserError', line)

    left.extend(right)

    return left


def clean_transcript_document(document):
    cut_the_introduction = lambda lines: lines[document.index("Otwieram posiedzenie Sejmu.") - 1:]
    document = " ".join(cut_the_introduction(document))

    # Regex replace patterns
    regexes = \
        [
            (re.compile(r'\(.*?\)'), ""),  # parenthesis
            (re.compile(r'(\s\s*)'), " "),  # excessive_spaces
            (re.compile(r'(\w+)- '), r"\1")
        ]  # line breaking

    print(len(document))
    for pattern, repl in regexes:
        document = re.sub(pattern, repl, document)

    return document


def extract_all_politician_info(government_n=9):
    url = rf'https://www.sejm.gov.pl/sejm{government_n}.nsf/'
    padding = lambda n: str(n).rjust(3, "0")

    all_politics = dict()
    politician_number = 1
    while True:
        try:
            politician_url = url + rf'posel.xsp?id={padding(politician_number)}&type=A'
            all_politics[politician_number] = scrape_politician_information(politician_url)
            politician_number += 1
        except StopIteration:
            break

    return all_politics


def scrape_politician_information(url):
    politician_dict = dict()

    web = requests.get(url)
    soup = bs.BeautifulSoup(web.content, 'html.parser')

    politician_dict["name"] = soup.find("title").get_text()

    if not politician_dict["name"]:
        raise StopIteration("URL doesn't contain any politician data.")
    else:
        print(politician_dict["name"], "fetched.")

    features = []
    values = []

    looking_for_feature = True
    for line in soup.findAll("p"):
        if line.get_attribute_list("class") == ["left"] and looking_for_feature:
            features.append(line.get_text().strip(":"))
            looking_for_feature = False
        elif line.get_attribute_list("class") == ["right"] and not looking_for_feature:
            values.append(line.get_text().strip(":"))
            looking_for_feature = True

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
        'Email': 'email',
        'Wygaśnięcie mandatu': 'resign_date',
        'Wybrana dnia': "election_date",
        'Tytuł/stopień naukowy': 'academic_degree'}

    features = map(lambda feature: feature_map.get(feature, feature), features)

    politician_dict.update(**dict(zip(features, values)))
    try:
        politician_dict["email"] = soup.findAll("p")[-1].find("a").get("href")
    except AttributeError:  # email is not available
        pass

    return clean_politician_data(politician_dict)


def clean_politician_data(politician_dict):
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
        'parliment_member': lambda value: re.findall(r'([XVI]+)', value).extend("IX"),
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
    politicians = extract_all_politician_info()
    import pprint

    pprint.pprint(politicians)