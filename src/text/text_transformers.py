"""
This module is responsible for tokenization of speeches. Token is a word or punctuation
that contains information about part of speech, lemmatizated version of the word and other
that will be useful in the modeling
"""

import spacy
import logging
import re
import pandas as pd
from functools import partial
from sklearn.feature_extraction.text import CountVectorizer


class LanguageCorpus:
    """
    Non-data descriptor of spacy model with lazy evaluation. It will store the model after first loading of it
    """

    def __init__(self):
        self.nlp = None

        self.ngrams = {'wysoki izba', 'prawo sprawiedliwość', 'unia europejski', 'ochrona zdrowie', 'finanse publiczny',
                       'koalicja obywatelski', 'trybunał konstytucyjny', 'andrzej duda', 'komisja europejski'}

        self.stopwords = {'ustawa', 'projekt', 'zmiana', 'poprawka', 'druk', 'przyjęcie', 'zapis', 'komisja',
                          'pani', 'być', 'marszałek', 'ten', 'to', 'na', 'nie', 'się', "poseł", "minister",
                          'który', 'że', 'do', 'dziękować', 'mieć', 'bardzo', 'oklask', "móc", "sejm",
                          'on', 'jak', 'czy', 'co', 'dzwonko', 'państwo', "wysoki_izba", "wysoki", "izba",
                          "poprawka", "zmiana", 'czytanie', 'przepis', 'posiedzenie', 'wniosek',
                          'uwaga', 'kwestia', 'strona', 'informacja', 'sprawa', "wicemarszałek",
                          "prosić", "powiedzieć", "chcieć", "mówić", "myślić", "chodzić", "rozwiązanie", "rząd",
                          "premiera", "czas", "cel", "zakres", "rok", "działanie", "rada"}

    def __get__(self, instance, cls):
        if instance is None:
            return self

        if self.nlp:
            return self.nlp
        else:
            self.load_model()
            self.update_stop_words()
            return self.nlp

    def load_model(self):
        try:
            self.nlp = spacy.load('pl_core_news_lg')
        except OSError:
            logging.getLogger('main').error("Language model not found. Download it using console:"
                                            'python -m spacy download pl_core_news_lg')
            raise

    def update_stop_words(self):
        self.nlp.Defaults.stop_words |= self.stopwords

        return corpus


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
                             ('speech_vector', doc.vector.round(2).tolist()),
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
                         ('is_numerical', bool(re.search(r'.*?\d.*', spacy_token.text)))])

        speech_details = [create_dict_with_word_details(ix, token) for ix, token in enumerate(spacy_document)]

        return speech_details


class SpeechAnalyzer:

    def __init__(self, speeches: dict):
        if 'speech_details' not in speeches:
            speeches = Tokenizer.transform(speeches)

        speech_transformation = partial(SpeechTransformer.transform,
                                        build_with="lemma",
                                        exclude_flags=["is_punct", "is_numerical"],
                                        include_params=["politician_id"])

        self.text_table = pd.DataFrame(data=list(map(speech_transformation, speeches)),
                                       columns=["politician", "text"])

    def find_n_grams(self, n):
        cv = CountVectorizer(ngram_range=(n, n))
        bag_of_words = cv.fit_transform(self.text_table["text"])

        ngrams = (pd.DataFrame(bag_of_words.sum(axis=0), columns=cv.get_feature_names())
                  .transpose()
                  .sort_values(0, ascending=False)
                  .head(200)
                  .index
                  .tolist())

        return ngrams

    def find_stop_words(self, threshold: float):
        cv = CountVectorizer()
        bag_of_words = cv.fit_transform(self.text_table["text"])
        df = pd.DataFrame(data=bag_of_words.toarray(),
                          columns=cv.get_feature_names(),
                          index=[id_ for id_, _ in self.text_table])

        # Clipping data, we are only checking existance of the word, not how many times it occur
        word_counts = df >= 1

        stop_words = ((word_counts.sum() / self.text_table.shape[0])
                      .sort_values(ascending=False)
                      > threshold)

        result = stop_words[stop_words].index
        return result


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


class SpeechTransformer:

    @staticmethod
    def transform(speech, build_with, allowed_pos=None, exclude_flags=None, include_params=None):

        def verify_if_should_be_excluded(word_dict, allowed_pos, exclude_flags):

            if exclude_flags:
                for flag in exclude_flags:
                    if word_dict[flag]:
                        return True

            if allowed_pos and word_dict["pos"] not in allowed_pos:
                return True

            return False

        result = []
        for word in speech["speech_details"]:
            if verify_if_should_be_excluded(word, allowed_pos, exclude_flags):
                continue
            result.append(word[build_with])

        return (*[speech[param] for param in include_params], result) if include_params else result


if __name__ == '__main__':
    import pickle
    from src.utils import get_project_structure
    from src.text import Tokenizer, BracketFinder

    STRUCTURE = get_project_structure()
    with open(STRUCTURE["test_folder"].joinpath("mock").joinpath("test_speeches.pickle"), "rb") as file:
        speeches = pickle.load(file)

    s = Tokenizer.transform(speeches)
    a = BracketFinder.transform(s)
