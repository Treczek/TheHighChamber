import pytest

from src.utils import swap_name_with_surname


@pytest.mark.parametrize('case, result',
                         [("Reczek Tomasz Michal", "Tomasz Michal Reczek"),
                          ("Reczek Tomasz", "Tomasz Reczek"),
                          ("Szynkowski vel Sęk Szymon", "Szymon Szynkowski vel Sęk")])
def test_swapping_name_with_surname(case, result):
    assert swap_name_with_surname(case) == result
