"""This file contains several helper functions to convert the csv data to star schema
so that this can be injected into the data warehouse.
These helper functions are designed to be run as a lambda function.

The file is run by calling this module with the python keyword.

Example:
    python src/dataframes.py

To run the test file, please use the below:
    pytest tests/test_dataframes.py
"""

# from src.extractor_lambda import put_tables_to_csv
import pandas as pd

# put_tables_to_csv()

def create_dim_staff():
    """Using pandas to read the staff.csv and department.csv files to merge and create
    the star schema dataframe.
    
    Args:
        Not required.

    Returns:
        Writes a star schema CSV file from the staff.csv and department.csv files,
        storing the dim_staff.csv in the tmp directory.

    Raises:
        ValueError: on KeyError - ERROR: dim_staff - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        staff_df = pd.read_csv('./tmp/staff.csv')
        dept_df = pd.read_csv('./tmp/department.csv')
        staff_dept_df = pd.merge(
            staff_df, dept_df, on='department_id').sort_values('staff_id')
        dim_staff = pd.DataFrame(data={'staff_id': staff_dept_df['staff_id'],
                                    'first_name': staff_dept_df['first_name'],
                                    'last_name': staff_dept_df['last_name'],
                                    'department_name': staff_dept_df['department_name'],
                                    'location': staff_dept_df['location'],
                                    'email_address': staff_dept_df['email_address']
                                    })
        dim_staff.to_csv('./tmp/dim_staff.csv', index=False)
        return dim_staff
    except KeyError as error:
        raise ValueError(f'ERROR: dim_staff - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')



def create_dim_design():
    """Using pandas to read the design.csv file to create the star schema dataframe.
    
    Args:
        Not required.

    Returns:
        Writes a star schema CSV file from the staff.csv and department.csv files,
        storing the dim_design.csv in the tmp directory.

    Raises:
        ValueError: on KeyError - ERROR: dim_design - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        des_df = pd.read_csv('./tmp/design.csv')
        dim_design = pd.DataFrame(data={'design_id': des_df['design_id'],
                                        'design_name': des_df['design_name'],
                                        'file_location': des_df['file_location'],
                                        'file_name': des_df['file_name']})
        dim_design.to_csv('./tmp/dim_design.csv', index=False)
        return dim_design
    except KeyError as error:
        raise ValueError(f'ERROR: dim_design - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')


def create_dim_location():
    """Using pandas to read the address.csv file to create the star schema dataframe.
    
    Args:
        Not required.

    Returns:
        Writes a star schema CSV file from the address.csv file,
        storing the dim_location.csv in the tmp directory.

    Raises:
        ValueError: on KeyError - ERROR: dim_location - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        loc_df = pd.read_csv('./tmp/address.csv')
        dim_location = pd.DataFrame(data={'location_id': loc_df['address_id'],
                                        'address_line_1': loc_df['address_line_1'],
                                        'address_line_2': loc_df['address_line_2'],
                                        'district': loc_df['district'],
                                        'city': loc_df['city'],
                                        'postal_code': loc_df['postal_code'], 
                                        'country': loc_df['country'],
                                        'phone': loc_df['phone']})
        dim_location.to_csv('./tmp/dim_location.csv', index=False)
        return dim_location
    except KeyError as error:
        raise ValueError(f'ERROR: dim_location - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

create_dim_staff()
create_dim_design()
create_dim_location()