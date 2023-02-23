from src.extractor_lambda import put_tables_to_csv
import pandas as pd
import os

# put_tables_to_csv()

# def get_file_names():
#     return sorted(os.listdir('tmp'))

# def read_csv_files():
#     file_data = []
#     for file_name in get_file_names():
#         file_data.append(pd.read_csv(f'./tmp/{file_name}'))
#     return file_data

def dim_staff():
    # staff_id, first_name, last_name, department_name, location, email_address
    pass

def dim_design():
    des_df = pd.read_csv('./tmp/design.csv')
    dim_design = pd.DataFrame(data={'design_id': des_df['design_id'], 'design_name': des_df['design_name'], 'file_location': des_df['file_location'], 'file_name': des_df['file_name']})
    dim_design.to_csv('./tmp/dim_design.csv', index=False)

def dim_location():
    # location_id, address_line_1, address_line_2, district, city, postal_code, country, phone
    pass
