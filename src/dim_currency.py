import pandas as pd
from src.extractor_lambda import put_tables_to_csv
put_tables_to_csv()

def make_dimension():

  """
  Summary: using pandas, read the currency csv injested from 
  the totesys DB. creates a new dataframe dataframe, changes the shorthand 
  currency code to the full name, formats in the schema required.

  Raises:
      ValueError: on KeyError - ERROR: dim_counterparty - "column" does not exist
      ValueError: on FileNotFoundError - missing files or directory
      ValueError: on pd.errors.EmptyDataError - files exist but are empty
      ValueError: on pd.errors.DtypeWarning - column data mismatch
      Exception: Blanket to catch unhandled error for cloudwatch
  """

  try:
    cur_df = pd.read_csv('./tmp/currency.csv')
  
    dim_currency = pd.DataFrame(data={'currency_id': cur_df['currency_id'], 'currency_code': cur_df['currency_code'], 'currency_name': cur_df['currency_code']})
    dim_currency['currency_name'] = dim_currency['currency_name'].str.replace('GBP', 'Pounds')
    dim_currency['currency_name'] = dim_currency['currency_name'].str.replace('USD', 'Dollars')
    dim_currency['currency_name'] = dim_currency['currency_name'].str.replace('EUR', 'Euros')
  
    dim_currency.to_csv('./tmp/dim_currency.csv', index=False)
  except KeyError as error:
    raise ValueError(f'ERROR: dim_currency - {error} does not exist')
  except FileNotFoundError as error:
    raise ValueError(f'ERROR: {error}')
  except pd.errors.EmptyDataError as EDE:
    raise ValueError(f'ERROR: {EDE}')
  except pd.errors.DtypeWarning as DTW:
    raise ValueError(f'ERROR: {DTW}')
  except Exception as error:
    raise ValueError(f'ERROR: {error}')