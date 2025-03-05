import functions_framework
import requests, json, os, datetime
from datetime import date
from collections import defaultdict
import math
#import sys, csv
from google.cloud import bigquery

def make_api_call(apikey, url, debug = False, allowErrors = False):
    headers = {
        'Content-type': 'application/json',
        'Fastly-Key': apikey
    }
    if debug:
        print("Calling API " + url)
    response_raw = requests.get(url, headers=headers)
    if response_raw.status_code != 200:
        if allowErrors == False:
            print('Unexpected status: %s response: %s' % (response_raw.status_code, response_raw.text))
            print(response_raw)
            exit()
        else:
            return []
    else:
        response = json.loads(response_raw.text)
        return response

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0 B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1000)))
   p = math.pow(1000, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

def get_service(apikey, sid):
    return make_api_call(apikey, "https://api.fastly.com/service/" + sid)

@functions_framework.http
def main(request):
    PROJECT_ID = "moz-fx-data-billing-prod-9147"
    DATASET_ID = "fastly"
    TABLE_ID = "fastly_breakdown"

    if not os.environ.get('FASTLY_KEY'):
      print("[ERROR] Please set your API key to environment variable FASTLY_KEY")
      exit(-1)
    else:
      apikey = os.environ['FASTLY_KEY']

    ###### DEFINE START AND END DATES HERE ######
    #start_date = date(2025, 2, 1)
    #end_date = date(2025, 2, 28)

    today = datetime.date.today()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - datetime.timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)

    start_date = first_day_prev_month
    end_date = last_day_prev_month

    services = {}
    final_results = {}
    total_bytes_delivered = 0
    
    fullURL = "https://api.fastly.com/stats/usage_by_service?from=" + start_date.strftime("%Y-%m-%d") + "T00:00:00.000000&to=" + end_date.strftime("%Y-%m-%d") + "T23:59:59.999999"
    try:
        response = make_api_call(apikey, fullURL, False, True)
    except Exception as e:
        print("Error calling url %s: %s\n" % (fullURL, e))
        exit()
    for region in response['data']:
        for sid in response['data'][region]:
            if sid not in services:
                services[sid] = get_service(apikey, sid)
            if sid not in final_results:
                keys = ['bandwidth', 'requests', 'compute_requests']
                default_value = 0
                final_results[sid] = defaultdict(lambda: default_value, {key: default_value for key in keys})
            final_results[sid]['bandwidth'] += response['data'][region][sid]['bandwidth']
            total_bytes_delivered += response['data'][region][sid]['bandwidth']
            final_results[sid]['requests'] += response['data'][region][sid]['requests'] + response['data'][region][sid]['compute_requests']

    print("------------------------Calculating data from {} to {}------------------------".format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
    rows_to_insert = []
    for sid in final_results:
        print("{} ({}) [{}] - {} ({:,} requests) - {}%".format(services[sid]['name'], sid, services[sid]['type'], convert_size(final_results[sid]['bandwidth']), final_results[sid]['requests'], round(100 * (final_results[sid]['bandwidth'] / total_bytes_delivered), 4)))
        rows_to_insert.append({
            "date": end_date.strftime('%Y-%m-%d'), 
            "service_name": services[sid]['name'], 
            "service_id": sid, 
            "service_type": services[sid]['type'], 
            "bandwidth_bytes": final_results[sid]['bandwidth'], 
            "requests": final_results[sid]['requests'],
            "percentage_of_total": round(100 * (final_results[sid]['bandwidth'] / total_bytes_delivered), 4)
        })
    print("{} total delivered".format(convert_size(total_bytes_delivered)))

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    # first, delete any previous versions of this data
    last_day_str = last_day_prev_month.strftime("%Y-%m-%d")

    # Specify your table identifier in the format: project.dataset.table
    table_id = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, TABLE_ID)

    query = f"""
    DELETE FROM `{table_id}`
    WHERE date = @last_day
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("last_day", "DATE", last_day_str)
        ]
    )

    # Execute the query and wait for it to complete
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    print(f"Deleted rows with invoice_day = {last_day_str}")

    errors = client.insert_rows_json(table_ref, rows_to_insert)

    # Check for errors
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Successfully inserted {len(rows_to_insert)} records into {DATASET_ID}.{TABLE_ID}")

    return 'OK'

if __name__ == "__main__":
   main()
