import pytest
import pickle
import numpy as np

from src.utils.file_utils import get_project_structure
from src.text import Tokenizer, BracketFinder


@pytest.fixture()
def speeches():
    with open(get_project_structure()["test_folder"].joinpath("mock").joinpath("test_speeches.pickle"), "rb") as file:
        return pickle.load(file)


@pytest.fixture()
def tokenized_speeches(speeches):

    tokenized_speeches = None

    def tokenize():
        return Tokenizer.transform(speeches)

    return tokenized_speeches if tokenized_speeches else tokenize()


@pytest.mark.parametrize('field', ['speech_details', 'speech_vector', 'sentences'])
def test_if_tokenizer_create_features(tokenized_speeches, field):
    assert all([field in speech for speech in tokenized_speeches])


@pytest.mark.parametrize('field, value_type', [('speech_details', list), ('speech_vector', np.ndarray), ('sentences', list)])
def test_if_tokenizer_create_features_with_types_as_expected(tokenized_speeches, field, value_type):
    assert all([type(speech[field]) is value_type for speech in tokenized_speeches])


@pytest.mark.parametrize('field', ['speech_details', 'speech_vector', 'sentences'])
def test_if_tokenizer_create_features_that_are_not_empty(tokenized_speeches, field):
    assert all([element is not None for speech in tokenized_speeches for element in speech[field]])


def test_if_bracket_finder_catch_all_brackets(tokenized_speeches):

    test_speeches = BracketFinder.transform(tokenized_speeches)

    for speech in test_speeches:
        catched_brackets = [word['text'] for word in speech['speech_details'] if word["inside_brackets"]].count("(")
        existed_brackets = [word['text'] for word in speech['speech_details']].count("(")
        assert catched_brackets == existed_brackets
