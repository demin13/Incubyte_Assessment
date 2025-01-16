import pandas as pd
import pytest

from etl_process import load_data, validate_data, add_derived_columns, partition_by_country

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "Customer_Name": ["Alex", "John"],
        "Customer_Id": [123457, 123458],
        "Open_Date": ["20101012", "20121013"],
        "Last_Consulted_Date": ["20121013", "20131014"],
        "Vaccination_Id": ["MVD", "MVD"],
        "Dr_Name": ["Paul", "Paul"],
        "State": ["SA", "TN"],
        "Country": ["USA", "IND"],
        "DOB": ["06031987", "07041985"],
        "Is_Active": ["A", "A"]
    })

def test_load_data():
    file_path = "data/test_file.txt"
    with open(file_path, "w") as f:
        f.write("|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|State|Country|DOB|Is_Active\n")
        f.write("|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A\n")
    data = load_data(file_path)
    assert not data.empty
    assert "Customer_Name" in data.columns

def test_validate_data(sample_data):
    valid_data = validate_data(sample_data)
    assert valid_data is not None

def test_add_derived_columns(sample_data):
    enriched_data = add_derived_columns(sample_data)
    assert "age" in enriched_data.columns
    assert "days_since_last_consulted" in enriched_data.columns

def test_partition_by_country(sample_data):
    country_data = partition_by_country(sample_data)
    assert "USA" in country_data
    assert "IND" in country_data
