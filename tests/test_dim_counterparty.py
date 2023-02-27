from bin.dim_counterparty import create_dim_counterparty
import pandas as pd
import pytest
import os

if not os.path.isdir('./tmp'):
      os.makedirs('./tmp')

def test_dataframe_contents_are_not_mutated():
  d = pd.DataFrame(data={'currency_id' : [1,2,3], 'currency_code': ['GBP','USD','EUR']})
  d.to_csv('./tmp/test_1.csv', index=False)

  converted = pd.read_csv('./tmp/test_1.csv')

  os.remove('./tmp/test_1.csv')
  pd.testing.assert_frame_equal(d, converted)

def test_emptyfile_raises_EmptyDataError():
  d = pd.DataFrame()
  d.to_csv('./tmp/EDEerrortest.csv', index=False)
  
  with pytest.raises(pd.errors.EmptyDataError):
    pd.read_csv('./tmp/EDEerrortest.csv')
  os.remove('./tmp/EDEerrortest.csv')

def test_missing_columns_raises_key_error():
  d = pd.DataFrame(data={'counterparty_id' : [1,2,3], 'legal_address_id':[1,2,3]})
  d.to_csv('./tmp/counterparty.csv', index=False)
  a = pd.DataFrame(data={'address_id' : [1,2,3], 'address_line_1':[1,2,3]})
  a.to_csv('./tmp/address.csv', index=False)
  
  with pytest.raises(ValueError):
    create_dim_counterparty()
  try:
      create_dim_counterparty()
  except Exception as e:
    assert e.args[0]== "ERROR: dim_counterparty - 'counterparty_legal_name' does not exist"

def test_converted_file_has_correct_column_names():
  d = pd.DataFrame(data={'counterparty_id' : [1,2,3], 'counterparty_legal_name': [2,3,4], 
                         'legal_address_id': [5,6,7], 'commercial_contact': [8,9,10],
                         'delivery_contact' : [1,2,3]})
  d.to_csv('./tmp/counterparty.csv', index=False)
  
  d = pd.DataFrame(data={'address_id' : [1,2,3], 'address_line_1': [2,3,4], 
                        'address_line_2': [5,6,7], 'district':[8,9,10],
                        'city':[1,2,3], 'postal_code':[4,5,6],
                        'country': [7,8,9], 'phone':[1,2,3]})
  d.to_csv('./tmp/address.csv', index=False)

  create_dim_counterparty()

  converted = pd.read_csv('./tmp/dim_counterparty.csv')

  columns = ['counterparty_id', 'counterparty_legal_name', 
             'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 
             'counterparty_legal_district', 'counterparty_legal_city',
             'counterparty_legal_postal_code', 'counterparty_legal_country',
             'counterparty_legal_phone_number']
  
  for name in columns:
    assert name in converted.columns

def test_converted_file_has_correctly_joined_addresses_on_address_id():
  d = pd.DataFrame(data={'counterparty_id' : [1,2], 'counterparty_legal_name': ['Fahey and Son','Leannon'], 
                         'legal_address_id': [15,28], 'commercial_contact': [8,9],
                         'delivery_contact' : [1,2]})
  d.to_csv('./tmp/counterparty.csv', index=False)
  
  d = pd.DataFrame(data={'address_id' : [1,2,15,28], 'address_line_1': ['6826 Herzog Via','179 Alexie Cliffs', '605 Haskell Trafficway', '079 Horacio Landing'], 
                        'address_line_2': ['','','Axel Freeway',''], 'district':[28441,99305-7380,10,1],
                        'city':[1,2,3,4], 'postal_code':[4,5,6,7],
                        'country': [7,8,9,10], 'phone':[1,2,3,4]})
  d.to_csv('./tmp/address.csv', index=False)

  create_dim_counterparty()

  converted = pd.read_csv('./tmp/dim_counterparty.csv')
  
  assert converted['counterparty_id'][0] == 1
  assert converted['counterparty_legal_address_line_1'][0] == '605 Haskell Trafficway'
  assert converted['counterparty_legal_address_line_2'][0] == 'Axel Freeway'
    
  assert converted['counterparty_id'][1] == 2
  assert converted['counterparty_legal_address_line_1'][1] == '079 Horacio Landing'

def test_cleanup():
  files = os.listdir('./tmp')
  for file in files:
      os.remove(f'./tmp/{file}')
  os.removedirs('./tmp')