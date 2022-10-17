import pytest
from get_data import get_dataset_db, get_naf_db
from prepare_data import prepare_dataset_db

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)



def test_get_dataset_db():
    """Tests the expected # of columns of dataset once retrievd from DB
    """
    assert get_dataset_db().shape[1] == 132

def test_get_naf_db():
    """Tests the expected # of columns of NAF table once retrieved from DB
    """
    assert get_naf_db().shape[1] == 11

def test_prepare_dataset_db():
    """Make sure that the final DataFrame after all preparation has the 
    expected number of columns
    """
    assert prepare_dataset_db().shape[1] == 116

