from bin.dim_currency import create_dim_currency
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

def test_converted_file_has_correct_column_names():
  d = pd.DataFrame(data={'currency_id' : [1,2,3], 'currency_code': ['GBP','USD','EUR']})
  d.to_csv('./tmp/currency.csv', index=False)

  create_dim_currency()

  converted = pd.read_csv('./tmp/dim_currency.csv')

  columns = ['currency_id', 'currency_code', 'currency_name']
  
  for name in columns:
    assert name in converted.columns

def test_emptyfile_raises_EmptyDataError():
  d = pd.DataFrame()
  d.to_csv('./tmp/EDEerrortest.csv', index=False)
  
  with pytest.raises(pd.errors.EmptyDataError):
    pd.read_csv('./tmp/EDEerrortest.csv')
  os.remove('./tmp/EDEerrortest.csv')

def test_missing_columns_raises_key_error():
  d = pd.DataFrame(data={'currency_id' : [1,2,3]})
  d.to_csv('./tmp/currency.csv', index=False)
  
  with pytest.raises(ValueError):
    create_dim_currency()
  try:
      create_dim_currency()
  except Exception as e:
    assert e.args[0]== "ERROR: dim_currency - 'currency_code' does not exist"
  
def test_cleanup():
  files = os.listdir('./tmp')
  for file in files:
      os.remove(f'./tmp/{file}')
  os.removedirs('./tmp')