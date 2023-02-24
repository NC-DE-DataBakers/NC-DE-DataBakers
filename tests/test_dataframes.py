from src.dataframes import create_dim_staff
import csv
import pytest
import os

if not os.path.exists('./tmp'):
    os.makedirs('./tmp')

def test_dim_staff_star_schema_contains_correct_column():
    correct_column_names = ['staff_id', 'first_name',
                            'last_name', 'department_name', 'location', 'email_address']
    dim_staff_table = create_dim_staff()
    staff_names = [key for key in dim_staff_table]
    assert staff_names == correct_column_names


def test_staff_department_id_correlates_with_department_name():
    department_look_up = {
        'Sales':	1,
        'Purchasing':	2,
        'Production':	3,
        'Dispatch':	4,
        'Finance':	5,
        'Facilities':	6,
        'Communications':	7,
        'HR':	8
    }

    with open('./tmp/staff.csv', 'r', encoding='utf-8') as csv_file:
        csv_data = csv.reader(csv_file)
        csv_list = [data for data in csv_data]

    with open('./tmp/dim_staff.csv', 'r', encoding='utf-8') as dim_staff_file:
        dim_staff_data = csv.reader(dim_staff_file)
        dim_staff_list = [data for data in dim_staff_data]

    for index in range(1, len(dim_staff_list)):
        compare_value = department_look_up[dim_staff_list[index][3]]
        assert compare_value == int(csv_list[index][3])
    