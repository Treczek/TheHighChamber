"""
This class will handle all queries to the database.
"""

import mongoengine
from src.mongo.schemas import Politician, Speech
from src.utils import swap_name_with_surname
import logging
import warnings
from json import JSONEncoder

from src.exceptions import NoPoliticianFound, DuplicatedNameWarning
from bson.json_util import default
from mongoengine import Document


class MongoManager:

    def __init__(self):

        self.global_init()
        self.log = logging.getLogger("main.mongo")

    @staticmethod
    def global_init():
        mongoengine.register_connection(alias="core", name="THC", host='mongodb://localhost:27017')

    def get_politician_by_name(self, name, try_swapped=True):
        """
        Args:
            name: politician name
            try_swapped: Boolean flag that indicates if lookup should also be done on reversed name with surname

        Returns: First politician object that is found in the mongo database

        """

        if len(p := Politician.objects(name=name)) > 1:
            warnings.warn(f"{name} appears more then once in the politician collection", DuplicatedNameWarning)
        elif len(p) == 0 and try_swapped:
            return self.get_politician_by_name(swap_name_with_surname(name), try_swapped=False)
        else:
            try:
                return p.first()
            except IndexError as exc:
                raise NoPoliticianFound(f"Politician {name} not found in the database.") from exc

        return p.first()

    def create_speech(self, speech):
        """
        Creates Speech document for given arguments
        Returns: Speech object
        """

        s = Speech()

        for key, value in speech.items():
            setattr(s, key, value)

        s.hash = s.generate_id()

        p = self.get_politician_by_name(s.politician_name)
        if p:
            s.politician_id = p.hash
        else:
            self.log.error(f"Speech of {s.politician_name} cannot be assign to a Politician.")

        return s

    def insert_politician(self, politician):
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
            self.log.debug(f"Politician {p.name} found in the database. Skipping.")
            return False

        p.save()
        self.log.debug(f"Politician {p.name} inserted to db")
        return True

    def insert_politicians(self, politicians):
        for politician in politicians:
            self.insert_politician(politician)

    @staticmethod
    def insert_speech(speech: Speech):
        p = Politician.objects(hash=speech.politician_id).first()
        if p and speech.hash not in [db_speech["hash"] for db_speech in p.speeches]:
            p.speeches.append(speech)
            p.save()
            return True
        return False

    def insert_speeches(self, speeches):

        speeches_objs = [self.create_speech(speech) for speech in speeches]
        insert_results = [self.insert_speech(speech) for speech in speeches_objs]
        if any(insert_results):
            self.log.info(
                f"Inserted {sum(insert_results)} speeches into db.")

    @staticmethod
    def get_last_speech_per_politician():
        result = list(Politician.objects().aggregate(
            [
                {'$unwind': "$speeches"},
                {'$group': {'_id': '$name', 'last_speech': {'$max': '$speeches.date'}}}
            ]
        ))

        return {dct["_id"]: dct["last_speech"] for dct in result}

    @staticmethod
    def get_speeches(filter_query=None):

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

    def update_speech(self, speech_hash, field_name, field_value, only_new):
        """
        This function can be used to find specific speech in the db and update it with the given value
        Args:
            speech_hash: hash which will be used to identify the speech
            field_name: name of the speech field that will be changed
            field_value: value for the given field. Type must be allowed by mongoDB
        Returns: None. Updates the speech in the database
        """

        # Mongoengine struggles with the updating of nested documents. The easiest way would be to use exec_js method
        # but it doesnt work with mongo > 4.0.
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
            self.log.debug(f"Speech with hash: {speech_hash} updated on field {field_name}")
        else:
            self.log.debug(f"Speech with hash: {speech_hash} not found in the database")
