import pandas as pd
from datetime import datetime

def load_data(file_path):
    column_names = [
        "Type", "Customer_Name", "Customer_Id", "Open_Date", "Last_Consulted_Date",
        "Vaccination_Id", "Dr_Name", "State", "Country", "DOB", "Is_Active"
    ]
    try:
        data = pd.read_csv(
            file_path, delimiter="|", header=None, names=column_names, on_bad_lines='warn'
        )
        data = data[data["Type"] == "D"].drop(columns=["Type"])
        return data
    except pd.errors.ParserError as e:
        print(f"ParserError: {e}")
        raise

def validate_data(data):
    mandatory_columns = ["Customer_Name", "Customer_Id", "Open_Date"]
    for column in mandatory_columns:
        if data[column].isnull().any():
            raise ValueError(f"Mandatory column {column} contains null values.")
    return data

def add_derived_columns(data):
    today = datetime.now()
    data["DOB"] = pd.to_datetime(data["DOB"], format="%d%m%Y", errors="coerce")
    data["Open_Date"] = pd.to_datetime(data["Open_Date"], format="%Y%m%d", errors="coerce")
    data["Last_Consulted_Date"] = pd.to_datetime(data["Last_Consulted_Date"], format="%Y%m%d", errors="coerce")

    data["age"] = today.year - data["DOB"].dt.year
    data["days_since_last_consulted"] = (today - data["Last_Consulted_Date"]).dt.days
    return data

def partition_by_country(data):
    return {country: group for country, group in data.groupby("Country")}

def save_to_tables(data_dict, output_dir):
    for country, df in data_dict.items():
        output_path = f"{output_dir}/Table_{country}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved data to {output_path}")

def etl_process():
    input_file = "data/customer_data.txt"
    output_dir = "output"

    data = load_data(input_file)
    data = validate_data(data)
    data = add_derived_columns(data)
    data = data[data["days_since_last_consulted"] > 30]
    country_data = partition_by_country(data)
    save_to_tables(country_data, output_dir)