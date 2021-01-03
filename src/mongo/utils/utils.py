import warnings
import logging

from src.mongo.schemas import Politician, Speech
from src.utils import swap_name_with_surname
from src.exceptions import NoPoliticianFound, DuplicatedNameWarning


# TODO TESTS
def find_politician_by_name(name, try_swapped=True):

    """
    Args:
        name: politician name
        try_swapped: Boolean flag that indicates if lookup should also be done on reversed name with surname

    Returns: First politician object that is found in the mongo database

    """

    if len(p := Politician.objects(name=name)) > 1:
        warnings.warn(f"{name} appears more then once in the politician collection", DuplicatedNameWarning)
    elif len(p) == 0 and try_swapped:
        return find_politician_by_name(swap_name_with_surname(name), try_swapped=False)
    else:
        try:
            return p.first()
        except IndexError as exc:
            raise NoPoliticianFound(f"Politician {name} not found in the database.") from exc

    return p.first()


def create_speech_object(politician_name, speech_date, speech_text):
    """
    Creates Speech document for given arguments
    Args:
        politician_name: full name of the speech author
        speech_date: date of speech in datetime type
        speech_text: full, raw text of the speech
    Returns: Speech object
    """

    s = Speech()
    s.date = speech_date
    s.raw_text = speech_text
    s.hash = s.generate_id()

    p = find_politician_by_name(politician_name)
    if p:
        s.politician_id = p.hash
    else:
        logging.getLogger("main.mongo").error(f"Speech of {politician_name} cannot be assign to a Politician.")

    return s


def create_politician_object(politician):
    """
    This function will create Politician object and save it to the mongo db
    Args:
        politician: dictionary of single politician scraped from government website
    """
    p = Politician()
    for key, value in politician.items():
        setattr(p, key, value)

    p.hash = p.generate_id()

    logging.getLogger("main.mongo").debug(f"Politician {p.name} has been created and ready to be inserted to db")

    return p


def insert_speech_into_db(speech: Speech):
    p = Politician.objects(hash=speech.politician_id).first()
    if speech.hash not in [db_speech["hash"] for db_speech in p.speeches]:
        p.speeches.append(speech)
        p.save()
        return True
    else:
        logging.getLogger("main.mongo").debug(f"Speech of {p.name} from {str(speech.date)} already in the database.")
        return False
