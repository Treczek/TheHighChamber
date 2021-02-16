"""Module for various functions used across whole project"""


def swap_name_with_surname(full_name):

    exceptions = {
        "Szynkowski vel Sęk Szymon": "Szymon Szynkowski vel Sęk",
        "Szymon Szynkowski vel Sęk": "Szymon Szynkowski vel Sęk"
    }

    if full_name in (in_keys := exceptions.keys()) or full_name in exceptions.values():
        return exceptions[full_name] if in_keys else {value: key for key, value in exceptions.items()}[full_name]

    name_parts = full_name.split(" ")
    swapped_full_name = list(name_parts[1:] + [name_parts[0]])

    return " ".join(swapped_full_name)
