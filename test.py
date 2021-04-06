from google.cloud import bigquery
client = bigquery.Client.from_service_account_json('C:/Users/diogo/PycharmProjects/process_facebook_leaks/facebookleaks-2f64eae2f889.json')
query = (f"create table if not exists `facebookleaks.facebookLeaks2021.test3` (numbers STRING,name STRING,gender STRING) ")
client.query(query)
#print(query_job.result())