"""
This module will contain functions that could filter speech objects into various states
"""


def build_lemmatizated_speech_with_politician_id(speech):
    politician = speech["politician_id"]
    lemma_text = " ".join([word["lemma"]
                           for word in speech["speech_details"]
                           if not word["is_punct"] and not word["is_numerical"]])
    return politician, lemma_text
