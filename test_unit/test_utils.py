import pytest

from src.utils import swap_name_with_surname


@pytest.mark.parametrize('case, result',
                         [("Reczek Tomasz Michal", "Tomasz Michal Reczek"),
                          ("Reczek Tomasz", "Tomasz Reczek"),
                          ("Reczek Tomasz vel Michal", "Tomasz vel Michal Reczek")])
def test_swapping_name_with_surname(case, result):
    assert swap_name_with_surname(case) == result
