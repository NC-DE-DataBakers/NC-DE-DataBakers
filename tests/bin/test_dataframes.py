from bin.dataframes import create_dim_staff, create_dim_design, create_dim_location
import csv
import pytest
import os
import pandas as pd

if not os.path.exists('./tmp'):
    os.makedirs('./tmp')

def test_dim_staff_star_schema_contains_correct_column():
    correct_column_names = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    dim_staff_table = create_dim_staff()
    staff_columns = [column for column in dim_staff_table]
    assert staff_columns == correct_column_names


def test_staff_department_id_correlates_with_department_name():
    staff_test_table = pd.DataFrame(data={  'staff_id': [1, 2, 3],
                                            'first_name': ['Jeremie', 'Deron', 'Jeanette'],
                                            'last_name': ['Franey', 'Beier', 'Erdman'],
                                            'department_id': [2, 6, 6],
                                            'email_address': ['jeremie.franey@terrifictotes.com', 'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com']})
    staff_test_table.to_csv('./tmp/staff_test_table.csv', index=False)

    dim_staff_test_table = pd.DataFrame(data={  'staff_id': [1, 2, 3],
                                                'first_name': ['Jeremie', 'Deron', 'Jeanette'],
                                                'last_name': ['Franey', 'Beier', 'Erdman'],
                                                'department_name': ['Purchasing', 'Facilities', 'Facilities'],
                                                'location': ['Manchester', 'Manchester', 'Manchester'],
                                                'email_address': ['jeremie.franey@terrifictotes.com', 'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com']})
    dim_staff_test_table.to_csv('./tmp/dim_staff_test_table.csv', index=False)

    department_look_up = {
        'Sales': 1,
        'Purchasing': 2,
        'Production': 3,
        'Dispatch':	4,
        'Finance': 5,
        'Facilities': 6,
        'Communications': 7,
        'HR': 8
    }

    with open('./tmp/staff_test_table.csv', 'r', encoding='utf-8') as csv_file:
        csv_data = csv.reader(csv_file)
        csv_list = [data for data in csv_data]

    with open('./tmp/dim_staff_test_table.csv', 'r', encoding='utf-8') as dim_staff_file:
        dim_staff_data = csv.reader(dim_staff_file)
        dim_staff_list = [data for data in dim_staff_data]

    for index in range(1, len(dim_staff_list)):
        compare_value = department_look_up[dim_staff_list[index][3]]
        assert compare_value == int(csv_list[index][3])

def test_dim_design_star_schema_contains_correct_column():
    correct_column_names = ['design_id', 'design_name', 'file_location', 'file_name']
    dim_design_table = create_dim_design()
    design_columns = [column for column in dim_design_table]
    assert design_columns == correct_column_names
    
def test_dim_location_star_schema_contains_correct_column():
    correct_column_names = ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    dim_location_table = create_dim_location()
    location_columns = [column for column in dim_location_table]
    assert location_columns == correct_column_names

def test_dim_staff_missing_columns_raises_value_error():
    staff_test_table = pd.DataFrame(data={  'staff_id': [1, 2, 3],
                                            'first_name': ['Jeremie', 'Deron', 'Jeanette'],
                                            'last_name': ['Franey', 'Beier', 'Erdman'],
                                            'department_id': [2, 6, 6]})
    staff_test_table.to_csv('./tmp/staff.csv', index=False)
  
    with pytest.raises(ValueError):
        create_dim_staff()
    try:
        create_dim_staff()
    except Exception as e:
        assert e.args[0]== "ERROR: dim_staff - 'email_address' does not exist"

def test_dim_design_missing_columns_raises_value_error():
    design_test_table = pd.DataFrame(data={ 'design_id': [1, 5, 8],
                                            'design_name': ['Wooden', 'Granite', 'Wooden'],
                                            'file_location': ['/home/user/dir', '/root', '/usr']})
    design_test_table.to_csv('./tmp/design.csv', index=False)
  
    with pytest.raises(ValueError):
        create_dim_design()
    try:
        create_dim_design()
    except Exception as e:
        assert e.args[0]== "ERROR: dim_design - 'file_name' does not exist"

def test_dim_location_missing_columns_raises_value_error():
    location_test_table = pd.DataFrame(data={   'address_id': [1, 2, 3],
                                                'address_line_1': ['6826 Herzog Via', '179 Alexie Cliffs', '148 Sincere Fort'],
                                                'district': ['Avon', '', ''],
                                                'city': ['New Patienceburgh', 'Aliso Viejo', 'Lake Charles'],
                                                'postal_code': ['28441', '99305-7380', '89360'],
                                                'country': ['Turkey', 'San Marino', 'Samoa'],
                                                'phone': ['1803 637401', '9621 880720', '0730 783349']})
    location_test_table.to_csv('./tmp/address.csv', index=False)
  
    with pytest.raises(ValueError):
        create_dim_location()
    try:
        create_dim_location()
    except Exception as e:
        assert e.args[0]== "ERROR: dim_location - 'address_line_2' does not exist"

def test_cleanup():
    files = os.listdir('./tmp')
    for file in files:
        os.remove(f'./tmp/{file}')
    os. removedirs ('./tmp')