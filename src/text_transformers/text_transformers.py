"""
This module is responsible for tokenization of speeches. Token is a word or punctuation
that contains information about part of speech, lemmatizated version of the word and other
that will be useful in the modeling
"""

import spacy
import logging


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


class Tokenizer:

    corpus = corpus

    @staticmethod
    def transform(speeches):
        for speech in speeches:
            Tokenizer.extract_speech_details(speech)
        return speeches

    @staticmethod
    def extract_speech_details(speech):
        """
        This function will extract all speeches from the database, tokenize it, and return them to mongo with updated
        additional attributes
        """

        doc = corpus.nlp(speech['raw_text'])

        for field, value in [('speech_details', Tokenizer.tokenize_speech(spacy_document=doc)),
                             ('speech_vector', doc.vector),
                             ('sentences', list(doc.sents))]:

            speech[field] = value

    @staticmethod
    def tokenize_speech(spacy_document):
        """
        This function will split given string object, process it using spacy and return list of
        dictionaries for each word or sign in the speech.
        """

        def create_dict_with_word_details(ix, spacy_token):

            return dict([('ix', ix),
                        ('text', spacy_token.text),
                        ('lemma', spacy_token.lemma_),
                        ('pos', spacy_token.pos_),
                        ('tag', spacy_token.tag_),
                        ('is_stop', spacy_token.is_stop),
                        ('is_punct', spacy_token.is_punct),
                        ('dep', spacy_token.dep_),
                        ('is_ent', bool(spacy_token.ent_type_)),
                        ('ent_label', spacy_token.ent_type_),
                        ('word_vector', spacy_token.vector)])

        speech_details = [create_dict_with_word_details(ix, token) for ix, token in enumerate(spacy_document)]

        return speech_details


class MarkNGrams:

    def transform(self):
        pass


class MarkStopWords:

    def transform(self):
        pass


class BracketFinder:

    @staticmethod
    def transform(speeches):

        assert all(['speech_details' in speech for speech in speeches])

        for speech in speeches:
            speech['speech_details'] = BracketFinder.add_flag_about_being_inside_brackets(speech['speech_details'])

        return speeches

    @staticmethod
    def add_flag_about_being_inside_brackets(speech_details):
        in_brackets = False
        for word_dct in speech_details:

            if word_dct["text"] == "(":
                in_brackets = True

            word_dct["inside_brackets"] = in_brackets

            if word_dct["text"] == ")":
                in_brackets = False

        return speech_details


if __name__ == '__main__':
    import pickle
    from src.utils import get_project_structure
    from src.text_transformers import Tokenizer, BracketFinder
    STRUCTURE = get_project_structure()
    with open(STRUCTURE["test_folder"].joinpath("mock").joinpath("test_speeches.pickle"), "rb") as file:
        speeches = pickle.load(file)

    s = Tokenizer.transform(speeches)
    a = BracketFinder.transform(s)


