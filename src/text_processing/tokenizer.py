"""
This module is responsible for tokenization of speeches. Token is a word or punctuation
that contains information about part of speech, lemmatizated version of the word and other
that will be useful in the modeling
"""

import spacy
import logging

from src.mongo.utils import get_all_speeches, update_speech


class LanguageCorpus:
    """
    Non-data descriptor of spacy model with lazy evaluation. It will store the model after first loading of it
    """

    def __init__(self):
        self.nlp = None

    def __get__(self, instance, cls):

        if instance is None:
            return None

        if self.nlp:
            return self.nlp
        else:
            self.load_model()
            return self.nlp

    def load_model(self):
        try:
            self.nlp = spacy.load('pl_core_news_lg')
        except OSError:
            logging.getLogger('main').error("Language model not found. Download it using console:"
                                            'python -m spacy download pl_core_news_lg')
            raise


class SpacyCorpus:
    nlp = LanguageCorpus()


corpus = SpacyCorpus()


def tokenize_speech(speech: str) -> dict:

    """
    This function will split given string object, process it using spacy and return list of
    dictionaries for each word or sign in the speech.
    """

    doc = corpus.nlp(speech)
    speech_details = []
    for ix, token in enumerate(doc):
        dict([('ix', ix),
              ('text', token.text),
              ('lemma', token.lemma_),
              ('pos', token.pos_),
              ('tag', token.tag_),
              ('is_stop', token.is_stop),
              ('is_punct', token.is_punct),
              ('dep', token.dep_),
              ('is_ent', bool(token.ent_type_)),
              ('ent_label', token.ent_type_)])

    return speech_details


def extract_speech_details(only_new=False):
    """
    This function will extract all speeches from the database, tokenize it, and return them to mongo with updated
    additional attributes
    """

    filter_query = {'speeches': {'$elemMatch': {'speech_details': {'$exists': False}}}} if only_new else None
    speeches = get_all_speeches(filter_query)

    for speech in speeches:
        speech_details = tokenize_speech(speech['speech'])
        update_speech(speech_hash=speech["speech_hash"], field_name="speech_details", field_value=speech_details)
