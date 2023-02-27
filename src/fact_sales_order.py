import pandas as pd
from datetime import date, timedelta
import os

def create_fact_sales_order():
    try:
        fact_sales = pd.read_csv('./tmp/sales_order.csv')

        # print(fact_sales.head(10))
        # print(fact_sales.info())

        fact_sales[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']] = fact_sales[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']].apply(pd.to_datetime, format="%Y-%m-%d %H:%M:%S.%f")

        # print(df.head(10))
        # print(df.info())
        # pd.to_datetime, format="%Y-%m-%d %H:%M:%S.%f")

        fact_sales['created_time'] = fact_sales['created_at'].dt.time
        fact_sales['last_updated_time'] = fact_sales['last_updated'].dt.time
        fact_sales['created_at'] = fact_sales['created_at'].dt.date
        fact_sales['last_updated'] = fact_sales['last_updated'].dt.date
        fact_sales['agreed_delivery_date'] = fact_sales['agreed_delivery_date'].dt.date
        fact_sales['agreed_payment_date'] = fact_sales['agreed_payment_date'].dt.date

        fact_sales.rename({'created_at': 'created_date', 'last_updated': 'last_updated_date', 'staff_id': 'sales_staff_id'}, axis=1, inplace=True)
        
        #print(fact_sales.head(10))
        #print(fact_sales.info())
        #print(type(fact_sales['created_date'][0]))

        fact_sales = fact_sales[['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']]
        print(fact_sales.head(5))
        # if not os.path.exists("pqt_tmp"):
        #     os.makedirs("pqt_tmp")
        # try:
        #   fact_sales.to_parquet(f'./pqt_tmp/fact_sales.parquet')
        # except pd.errors.EmptyDataError as EDE:
        #   raise f'ERROR: {EDE}'
        fact_sales.to_csv('./tmp/fact_sales_order.csv', index=False)

        # except Exception:
        #     raise f'ERROR: {Exception}'
    except KeyError as error:
        raise ValueError(f'ERROR: dim_counterparty - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except AttributeError as Attr:
        raise AttributeError(f'ERROR: {Attr}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')


def create_dim_date():
    try:
        df = pd.read_csv('./tmp/sales_order.csv')

        # print(df.head(10))
        # print(df.info())

        df[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']] = df[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']] .apply(pd.to_datetime, format="%Y-%m-%d %H:%M:%S.%f")
        
        min_dates = []
        max_dates = []

        min_dates.append(df['created_at'].min())
        min_dates.append(df['last_updated'].min())
        min_dates.append(df['agreed_delivery_date'].min())
        min_dates.append(df['agreed_payment_date'].min())

        max_dates.append(df['created_at'].max())
        max_dates.append(df['last_updated'].max())
        max_dates.append(df['agreed_delivery_date'].max())
        max_dates.append(df['agreed_payment_date'].max())

        min_date = min(min_dates)
        max_date = max(max_dates)
        
        # print(min_date)
        # print(max_date)
        dim_date = pd.DataFrame()
        dim_date['date_id'] = [min_date+timedelta(days=x) for x in range(((max_date + timedelta(days=1))-min_date).days)]

        dim_date['year'] = dim_date['date_id'].dt.year
        dim_date['month'] = dim_date['date_id'].dt.month
        dim_date['day'] = dim_date['date_id'].dt.day
        dim_date['day_of_week'] = dim_date['date_id'].dt.day_of_week
        dim_date['day_name'] = dim_date['date_id'].dt.day_name()
        dim_date['month_name'] = dim_date['date_id'].dt.month_name()
        dim_date['quarter'] = dim_date['date_id'].dt.quarter

        # print(dim_date.head(5))
        # print(dim_date.tail(5))
        
        # if not os.path.exists("pqt_tmp"):
        #     os.makedirs("pqt_tmp")
        # try:
        # dim_date.to_parquet(f'./pqt_tmp/dim_date.parquet')
        # except pd.errors.EmptyDataError as EDE:
        # raise f'ERROR: {EDE}'
        # except Exception:
        # raise f'ERROR: {Exception}'
        dim_date.to_csv('./tmp/dim_date.csv', index=False)

    except KeyError as error:
        raise ValueError(f'ERROR: dim_counterparty - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except AttributeError as Attr:
        raise AttributeError(f'ERROR: {Attr}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')
create_fact_sales_order()
create_dim_date()