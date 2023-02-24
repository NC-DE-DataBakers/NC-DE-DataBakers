import pandas as pd

def make_dimension():
  """
  Summary: using pandas, read the counterparty and address csv injested from 
  the totesys DB,  merges the two dataframes on the address_id. creates a new 
  dataframe from the merged data in the schema required

  Raises:
      ValueError: on KeyError - ERROR: dim_counterparty - "column" does not exist
      ValueError: on FileNotFoundError - missing files or directory
      ValueError: on pd.errors.EmptyDataError - files exist but are empty
      ValueError: on pd.errors.DtypeWarning - column data mismatch
      Exception: Blanket to catch unhandled error for cloudwatch
  """
  try:
    counter_party_df = pd.read_csv('./tmp/counterparty.csv')
    address = pd.read_csv('./tmp/address.csv')
    counter_party_df = counter_party_df.rename(columns={"legal_address_id":"address_id"})
    merged_df = pd.merge(counter_party_df, address, on='address_id')
   
    dim_counterparty = pd.DataFrame(data={'counterparty_id': merged_df['counterparty_id'], 
                                      'counterparty_legal_name': merged_df['counterparty_legal_name'],
                                      'counterparty_legal_address_line_1' : merged_df['address_line_1'], 
                                      'counterparty_legal_address_line_2' : merged_df['address_line_2'], 
                                      'counterparty_legal_district': merged_df['district'],
                                      'counterparty_legal_city':merged_df['city'],
                                      'counterparty_legal_postal_code':merged_df['postal_code'],
                                      'counterparty_legal_country': merged_df['country'],
                                      'counterparty_legal_phone_number': merged_df['phone']}).sort_values(by=['counterparty_id'])

    dim_counterparty.to_csv('./tmp/dim_counterparty.csv', index=False)
  except KeyError as error:
    raise ValueError(f'ERROR: dim_counterparty - {error} does not exist')
  except FileNotFoundError as error:
    raise ValueError(f'ERROR: {error}')
  except pd.errors.EmptyDataError as EDE:
    raise ValueError(f'ERROR: {EDE}')
  except pd.errors.DtypeWarning as DTW:
    raise ValueError(f'ERROR: {DTW}')
  except Exception as error:
    raise ValueError(f'ERROR: {error}')
  

  
  
  
