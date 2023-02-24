from src.extractor_lambda import put_tables_to_csv
import pandas as pd
import os

# put_tables_to_csv()


def create_dim_staff():
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


def create_dim_design():
    des_df = pd.read_csv('./tmp/design.csv')
    dim_design = pd.DataFrame(data={'design_id': des_df['design_id'], 'design_name': des_df['design_name'],
                              'file_location': des_df['file_location'], 'file_name': des_df['file_name']})
    dim_design.to_csv('./tmp/dim_design.csv', index=False)


def create_dim_location():
    loc_df = pd.read_csv('./tmp/address.csv')
    dim_location = pd.DataFrame(data={'location_id': loc_df['address_id'], 'address_line_1': loc_df['address_line_1'], 'address_line_2': loc_df['address_line_2'],
                                'district': loc_df['district'], 'city': loc_df['city'], 'postal_code': loc_df['postal_code'], 'country': loc_df['country'], 'phone': loc_df['phone']})
    dim_location.to_csv('./tmp/dim_location.csv', index=False)


