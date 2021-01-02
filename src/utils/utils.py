"""Module for various functions used across whole project"""


def swap_name_with_surname(full_name):
    name_parts = full_name.split(" ")
    swapped_full_name = list(name_parts[1:] + [name_parts[0]])
    return " ".join(swapped_full_name)

print(swap_name_with_surname("Reczek Tomasz Michał"))
print(swap_name_with_surname("Reczek Tomasz"))

