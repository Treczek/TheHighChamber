"""
Module with util functions related to files and directories
"""

import pickle
import pathlib


def pickle_obj(obj, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(obj, file, protocol=pickle.HIGHEST_PROTOCOL)


def read_pickle(file_path):
    with open(file_path, "rb") as file:
        obj = pickle.load(file)
    return obj


# TODO unit tests
def get_project_structure():
    """
    Returns: Dictionary with pathlib.Path objects with important project directories.
    """
    p = pathlib.Path.cwd()

    if p.name == "src":
        root = p.parent
    elif "src" in [path.name for path in p.iterdir()]:
        root = p
    else:
        while p.name != "src":
            p = p.parent
        root = p.parent

    project_structure = dict(root=root,
                             source=root.joinpath("src"),
                             backup=root.joinpath("backup"),
                             log_folder=root.joinpath("logs"))

    for path in project_structure.values():
        path.mkdir(parents=True, exist_ok=True)

    return project_structure
