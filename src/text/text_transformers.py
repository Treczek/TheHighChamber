"""
This module is responsible for tokenization of speeches. Token is a word or punctuation
that contains information about part of speech, lemmatizated version of the word and other
that will be useful in the modeling
"""

import spacy
import logging
import re
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

from src.text import build_lemmatizated_speech_with_politician_id


class LanguageCorpus:
    """
    Non-data descriptor of spacy model with lazy evaluation. It will store the model after first loading of it
    """

    def __init__(self):
        self.nlp = None

        self.ngrams = {'wysoki izba', 'prawo sprawiedliwość', 'unia europejski', 'ochrona zdrowie', 'finanse publiczny',
                       'koalicja obywatelski', 'trybunał konstytucyjny', 'andrzej duda', 'komisja europejski'}

        self.stopwords = ('pani', 'być', 'marszałek', 'ten', 'to', 'na', 'nie', 'wysoki', 'się',
                          'który', 'że', 'do', 'dziękować', 'izba', 'mieć', 'bardzo', 'oklask',
                          'on', 'jak', 'czy', 'co', 'dzwonko', 'państwo')

    def __get__(self, instance, cls):
        if instance is None:
            return None

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
                         ('word_vector', spacy_token.vector),
                         ('is_numerical', bool(re.search(r'.*?\d.*', spacy_token.text)))])

        speech_details = [create_dict_with_word_details(ix, token) for ix, token in enumerate(spacy_document)]

        return speech_details


class SpeechAnalyzer:

    def __init__(self, speeches: dict):
        if 'speech_details' not in speeches:
            speeches = Tokenizer.transform(speeches)

        self.text_table = pd.DataFrame(
            data=list(map(build_lemmatizated_speech_with_politician_id, speeches)),
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

        print(ngrams)
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
        print(result)
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


if __name__ == '__main__':
    import pickle
    from src.utils import get_project_structure
    from src.text import Tokenizer, BracketFinder

    STRUCTURE = get_project_structure()
    with open(STRUCTURE["test_folder"].joinpath("mock").joinpath("test_speeches.pickle"), "rb") as file:
        speeches = pickle.load(file)

    s = Tokenizer.transform(speeches)
    a = BracketFinder.transform(s)
