from src.dataframes import get_file_names, read_csv_files, tmp_exists
import csv
import pytest
import os

if not os.path.exists('./tmp'):
    os.makedirs('./tmp')

def test_get_file_names_returns_a_list_of_all_file_names_in_tmp_directory():
    file_names = ['purchase_order.csv', 'sales_order.csv', 'counterparty.csv', 'staff.csv', 'currency.csv', 'department.csv', 'address.csv', 'payment.csv', 'payment_type.csv', 'transaction.csv', 'design.csv']
    for file_name in file_names:
        assert file_name in get_file_names()

def test_dataframe_and_csv_file_contains_same_length_of_columns():
    with open('./tmp/staff.csv', 'r', encoding = 'utf-8') as csv_file:
        csv_data = csv.reader(csv_file)
        csv_list = [data for data in csv_data]
        pd_data = read_csv_files()
        pd_list = [df for df in pd_data[9]]
        assert len(csv_list[0]) == len(pd_list)