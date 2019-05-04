import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

raw_dataset = 'econ_raw' # dataset with raw tables
crypto_dataset = 'crypto_market_data_24H'
range_dataset = 'econ_range'
new_dataset = 'econ_airflow' # empty dataset for destination tables
sql_cmd_start = 'bq query --use_legacy_sql=false '

sql_Amazon = 'create table ' + new_dataset + '.Amazon_Temp as select * ' \
           'from ' + raw_dataset + '.gafa_stock_prices ' \
           'where Stock = Amazon '

sql_Apple = 'create table ' + new_dataset + '.Apple_Temp as ' \
            'select * ' \
            'from ' + raw_dataset + '.gafa_stock_prices ' \
            'where Stock = Apple '

sql_Facebook = 'create table ' + new_dataset + '.Facebook_Temp as ' \
            'select * ' \
            'from ' + raw_dataset + '.gafa_stock_prices ' \
            'where Stock = Facebook '

sql_Amazon_Range = 'create table ' + range_dataset + '.Amazon_Range_Temp as ' \
                  'select Amazon.ID as ID, (Amazon.Open - Amazon.Close) as Range ' \
                  'from ' + new_dataset + '.Amazon'

sql_Apple_Range = 'create table ' + range_dataset + '.Apple_Range_Temp as ' \
                  'select Apple.ID as ID, (Apple.Open - Apple.Close) as Range ' \
                  'from ' + new_dataset + '.Apple'

sql_Facebook_Range = 'create table ' + range_dataset + '.Facebook_Range_Temp as ' \
                  'select Facebook.ID as ID, (Facebook.Open - Facebook.Close) as Range ' \
                  'from ' + new_dataset + '.Facebook'

sql_Amazon_Range_All = 'create table ' + new_dataset + '.Amazon_Range_All_Temp as ' \
                  'select Amazon.ID as ID, Amazon.Stock as Stock, Amazon.Date as Date, Amazon.Open as Open, Amazon.High as High, Amazon.Low as Low, ' \
                  'Amazon.Close as Close, Amazon.Volume as Volume, Amazon_Range.Range as Range ' \
                  'from ' + new_dataset + '.Amazon as Amazon ' \
                  'join ' + range_dataset + '.Amazon_Range as Amazon_Range ' \
                  'on Amazon.ID = Amazon_Range.ID' \
                  'order by Amazon.ID'

sql_Apple_Range_All = 'create table ' + new_dataset + '.Apple_Range_All_Temp as ' \
                  'select Apple.ID as ID, Apple.Stock as Stock, Apple.Date as Date, Apple.Open as Open, Apple.High as High, Apple.Low as Low, ' \
                  'Apple.Close as Close, Apple.Volume as Volume, Apple_Range.Range as Range ' \
                  'from ' + new_dataset + '.Apple as Apple ' \
                  'join ' + range_dataset + '.Apple_Range as Apple_Range ' \
                  'on Apple.ID = Apple_Range.ID ' \
                  'order by Apple.ID'

sql_Facebook_Range_All = 'create table ' + new_dataset + '.Facebook_Range_All_Temp as ' \
                  'select Facebook.ID as ID, Facebook.Stock as Stock, Facebook.Date as Date, Facebook.Open as Open, Facebook.High as High, Facebook.Low as Low, ' \
                  'Facebook.Close as Close, Facebook.Volume as Volume, Facebook_Range.Range as Range ' \
                  'from ' + new_dataset + '.Facebook as Facebook ' \
                  'join ' + range_dataset + '.Facebook_Range as Facebook_Range ' \
                  'on Facebook.ID = Facebook_Range.ID ' \
                  'order by Facebook.ID'

sql_Amazon_rise_crypto_drop = 'create table ' + range_dataset + '.Amazon_rise_crypto_drop as ' \
            'SELECT Amazon.Date as Date, Amazon.Range as Amazon_Range, BTC.Range as BTC_Range, ETH.Range as ETH_Range, LTC.Range as LTC_Range' \
            'from ' + range_dataset + '.Amazon as Amazon ' \
            'join ' + crypto_dataset + '.BTC_24H_2017_Range as BTC ' \
            'on Amazon.Date = BTC.Date' \
            'join ' + crypto_dataset + '.ETH_24H_2017_Range as ETH ' \
            'on Amazon.Date = ETH.Date' \
            'join ' + crypto_dataset + '.LTC_24H_2017_Range as LTC ' \
            'on Amazon.Date = LTC.Date' \
            'order by Amazon.Date'

sql_Apple_rise_crypto_drop = 'create table ' + range_dataset + '.Apple_rise_crypto_drop as ' \
            'SELECT Apple.Date as Date, Apple.Range as Apple_Range, BTC.Range as BTC_Range, ETH.Range as ETH_Range, LTC.Range as LTC_Range' \
            'from ' + range_dataset + '.Apple as Apple ' \
            'join ' + crypto_dataset + '.BTC_24H_2017_Range as BTC ' \
            'on Apple.Date = BTC.Date' \
            'join ' + crypto_dataset + '.ETH_24H_2017_Range as ETH ' \
            'on Apple.Date = ETH.Date' \
            'join ' + crypto_dataset + '.LTC_24H_2017_Range as LTC ' \
            'on Apple.Date = LTC.Date' \
            'order by Apple.Date'

sql_Facebook_rise_crypto_drop = 'create table ' + range_dataset + '.Facebook_rise_crypto_drop as ' \
            'SELECT Facebook.Date as Date, Facebook.Range as Facebook_Range, BTC.Range as BTC_Range, ETH.Range as ETH_Range, LTC.Range as LTC_Range' \
            'from ' + range_dataset + '.Facebook as Facebook ' \
            'join ' + crypto_dataset + '.BTC_24H_2017_Range as BTC ' \
            'on Facebook.Date = BTC.Date' \
            'join ' + crypto_dataset + '.ETH_24H_2017_Range as ETH ' \
            'on Facebook.Date = ETH.Date' \
            'join ' + crypto_dataset + '.LTC_24H_2017_Range as LTC ' \
            'on Facebook.Date = LTC.Date' \
            'order by Facebook.Date'


with models.DAG(
        'econ_workflow1',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    create_amazon_table = BashOperator(
            task_id='create_amazon_table',
            bash_command=sql_cmd_start + '"' + sql_Amazon + '"')

    create_apple_table = BashOperator(
            task_id='create_apple_table',
            bash_command=sql_cmd_start + '"' + sql_Apple + '"')

    create_facebook_table = BashOperator(
            task_id='create_facebook_table',
            bash_command=sql_cmd_start + '"' + sql_Facebook + '"')

    create_amazon_range_table = BashOperator(
            task_id = 'create_amazon_range_table',
            bash_command =sql_cmd_start + '"' + sql_Amazon_Range_All + '"')

    create_apple_range_table = BashOperator(
            task_id = 'create_apple_range_table',
            bash_command =sql_cmd_start + '"' + sql_Apple_Range_All + '"')

    create_facebook_range_table = BashOperator(
            task_id = 'create_facebook_range_table',
            bash_command =sql_cmd_start + '"' + sql_Facebook_Range_All + '"')

    create_amazon_rise_crypto_drop_table = BashOperator(
            task_id = 'create_amazon_rise_crypto_drop_table',
            bash_command =sql_cmd_start + '"' + sql_Amazon_rise_crypto_drop + '"')

    create_apple_rise_crypto_drop_table = BashOperator(
            task_id = 'create_apple_rise_crypto_drop_table',
            bash_command =sql_cmd_start + '"' + sql_Apple_rise_crypto_drop + '"')

    create_facebook_rise_crypto_drop_table = BashOperator(
            task_id = 'create_facebook_rise_crypto_drop_table',
            bash_command =sql_cmd_start + '"' + sql_Facebook_rise_crypto_drop + '"')

    create_amazon_table >> create_apple_table >> create_facebook_table >> create_amazon_range_table >> create_apple_range_table >> create_facebook_range_table \
    >> create_amazon_rise_crypto_drop_table >> create_apple_rise_crypto_drop_table >> create_facebook_rise_crypto_drop_table
