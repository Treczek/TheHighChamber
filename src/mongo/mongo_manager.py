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

    def insert_speech(self, speech: Speech):
        p = Politician.objects(hash=speech.politician_id).first()
        if not speech.hash not in p.speeches:
            p.speeches.append(speech.hash)
            p.save()
            speech.save()
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
                {'$group': {'_id': '$politician_name', 'last_speech': {'$max': '$date'}}}
            ]
        ))

        return {dct["_id"]: dct["last_speech"] for dct in result}

