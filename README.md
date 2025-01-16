# Incubyte Assessment

This project implements an ETL process using Python with Pandas and NumPy modules, adhering to clean coding practices and test-driven development (TDD). The ETL pipeline processes customer data files, partitions them by country, and saves the output into separate files for each country.

## Features
- Load data from a file.
- Validate data for required fields.
- Add derived columns: `age` and `days_since_last_consulted`.
- Filter records where `days_since_last_consulted > 30`.
- Partition data into country-specific groups.
- Save each partition to respective country files.
- Test coverage for all key functions using `pytest`.

## File Structure
```
.
|-- main.py                 # Entry point for running the ETL process
|-- etl_process.py          # Core ETL functions
|-- test_etl_process.py     # Unit tests for ETL functions
|-- requirements.txt        # Dependencies for the project
|-- README.md               # Documentation
|-- data/                   # Input data files
|-- output/                 # Output files for each country
```

## Prerequisites
1. **Python**: Ensure Python 3.8 or higher is installed.
2. **Install dependencies**: Install the required Python packages using `pip`.

```bash
pip install -r requirements.txt
```

## How to Run
1. Place your input data file in the `data/` directory. Ensure the file follows the specified format with `|` as the delimiter.
2. Run the ETL process using the `main.py` script.

```bash
python main.py
```

3. Processed files will be saved in the `output/` directory with filenames like `Table_<Country>.csv`.

## Running Tests
To ensure the ETL process works correctly, unit tests are provided in `test_etl_process.py`.

Run tests using `pytest`:
```bash
pytest test_etl_process.py
```

Test output will indicate whether all functions are working as expected.

## Project Example
### Input File (`data/sample_input.txt`):
```
|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|State|Country|DOB|Is_Active
|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A
|D|John|123458|20101012|20131014|MVD|Paul|TN|IND|07041985|A
```

### Output Files:
- `output/Table_USA.csv`:
  ```csv
  Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active,age,days_since_last_consulted
  Alex,123457,2010-10-12,2012-10-13,MVD,Paul,SA,USA,1987-03-06,A,36,4123
  ```

- `output/Table_IND.csv`:
  ```csv
  Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active,age,days_since_last_consulted
  John,123458,2010-10-12,2013-10-14,MVD,Paul,TN,IND,1985-04-07,A,38,3399
  ```

## Clean Coding Practices
- Modular functions with clear responsibilities.
- Meaningful variable and function names.
- Comprehensive error handling for validations.
- Adherence to PEP 8 standards.

## Contact
For questions or feedback, please contact [Shekhar Anand](mailto:shekhar4321anand@gmail.com).
