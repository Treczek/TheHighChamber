import logging
import warnings
from json import JSONEncoder
import itertools

from bson.json_util import default
from mongoengine import Document

from src.exceptions import NoPoliticianFound, DuplicatedNameWarning
from src.mongo.schemas import Politician, Speech
from src.utils import swap_name_with_surname


class MongoEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Document):
            return o.to_mongo()
        return default(o)


# TODO TESTS
def get_politician_by_name(name, try_swapped=True):
    """
    Args:
        name: politician name
        try_swapped: Boolean flag that indicates if lookup should also be done on reversed name with surname

    Returns: First politician object that is found in the mongo database

    """

    if len(p := Politician.objects(name=name)) > 1:
        warnings.warn(f"{name} appears more then once in the politician collection", DuplicatedNameWarning)
    elif len(p) == 0 and try_swapped:
        return get_politician_by_name(swap_name_with_surname(name), try_swapped=False)
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

    p = get_politician_by_name(politician_name)
    if p:
        s.politician_id = p.hash
    else:
        logging.getLogger("main.mongo").error(f"Speech of {politician_name} cannot be assign to a Politician.")

    return s


def insert_politician_to_db(politician):
    """
    This function will create Politician object and save it to the mongo db
    Args:
        politician: dictionary of single politician scraped from government website
    """
    p = Politician()
    for key, value in politician.items():
        setattr(p, key, value)

    p.hash = p.generate_id()

    if Politician.objects(hash=p.hash).first():
        logging.getLogger("main.mongo").debug(f"Politician {p.name} found in the database. Skipping.")
        return False

    p.save()
    logging.getLogger("main.mongo").debug(f"Politician {p.name} inserted to db")
    return True


def insert_speech_into_db(speech: Speech):
    p = Politician.objects(hash=speech.politician_id).first()
    if p and speech.hash not in [db_speech["hash"] for db_speech in p.speeches]:
        p.speeches.append(speech)
        p.save()
        return True
    return False


def get_last_speech_per_politician():
    result = list(Politician.objects().aggregate(
        [
            {'$unwind': "$speeches"},
            {'$group': {'_id': '$name', 'last_speech': {'$max': '$speeches.date'}}}
        ]
    ))

    return {dct["_id"]: dct["last_speech"] for dct in result}


def get_speech_from_hash(speech_hash):
    all_speeches_per_politician = Politician.objects.scalar('speeches')
    for speech in itertools.chain.from_iterable(all_speeches_per_politician):
        if speech.hash == speech_hash:
            "Speech found, we can brak the for loop and return the object"
            break
    else:
        error_msg = "There is no speech with hash " + speech_hash
        logging.getLogger('mongo').error(error_msg)
        raise AttributeError(error_msg)

    return speech


def get_all_speeches(filter_query=None):

    if filter_query is None:
        filter_query = {}

    result = list(Politician.objects().aggregate(
        [
            {'$match': filter_query},
            {'$unwind': "$speeches"},
            {'$project': {'speech_hash': '$speeches.hash',
                          'name': '$name',
                          'speech': '$speeches.raw_text'}}
        ]
    ))

    return result


def update_speech(speech_hash, field_name, field_value):
    """
    This function can be used to find specific speech in the db and update it with the given value
    Args:
        speech_hash: hash which will be used to identify the speech
        field_name: name of the speech field that will be changed
        field_value: value for the given field. Type must be allowed by mongoDB
    Returns: None. Updates the speech in the database
    """

    # Mongoengine struggles with the updating of nested documents. The easiest way would be to use exec_js method but it
    # doesnt work with mongo > 4.0.
    # We will use private method to grab the collection object and use pymongo instead.

    politician = Politician._get_collection()

    filter_query = \
        {'speeches': {'$elemMatch': {'hash': speech_hash}}} \
        if only_new \
        else {'speeches': {'$elemMatch': {'hash': speech_hash, field_name: {'$exists': False}}}}

    result = politician.update_one(
        filter=filter_query,
        update={"$set": {f"speeches.$[element].{field_name}": field_value}},
        array_filters=[{'element.hash': speech_hash}])

    if result.matched_count:
        logging.getLogger('mongo').debug(f"Speech with hash: {speech_hash} updated on field {field_name}")
    else:
        logging.getLogger('mongo').debug(f"Speech with hash: {speech_hash} not found in the database")
