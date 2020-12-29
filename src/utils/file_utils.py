"""
Module with util functions related to files and directories
"""

import pickle


def pickle_obj(obj, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(obj, file, protocol=pickle.HIGHEST_PROTOCOL)
