import webapp2, json, logging, os

from pprint import pprint

from google.cloud import bigquery
from google.appengine.api import memcache, taskqueue

from datetime import date, timedelta
import time, uuid

## https://cloud.google.com/bigquery/streaming-data-into-bigquery
## function to send data to BQ stream
## https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/cloud-client/stream_data.py
def stream_data(dataset_name, table_name, json_data, time_stamp = time.time()):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    data = json_data

    data['ts'] = time_stamp

    # Reload the table to get the schema.
    table.reload()

    rows = [data]

    logging.debug(rows)
    # https://googlecloudplatform.github.io/google-cloud-python/stable/bigquery-table.html#google.cloud.bigquery.table.Table.insert_data
    errors = table.insert_data(rows, row_ids = str(uuid.uuid4()), ignore_unknown_values=False)

    if not errors:
    	logging.debug('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
    else:
        logging.error(errors)

class MainHandler(webapp2.RequestHandler):
	def get(self):
		## allows CORS
		self.response.headers.add_header("Access-Control-Allow-Origin", "*")

		## get example.com?bq=blah
		b = self.request.get("bq")

		## send to async task URL
		task = taskqueue.add(url='/bq-task', params={'bq': b, '_ts': str(time.time())})

class BqHandler(webapp2.RequestHandler):
	def post(self):

		## get example.com/bq-task?bq=blah
		b = self.request.get("bq")
		ts = self.request.get("_ts")

		b = json.loads(b)

		logging.debug(b)

		if len(b) > 0:
			datasetId = os.environ['DATASET_ID']
			tableId   = os.environ['TABLE_ID']

			today = date.today().strftime("%Y%m%d")

			tableId = "%s$%s"%(tableId, today)

			stream_data(datasetId, tableId, b, ts)


app = webapp2.WSGIApplication([
    ('/bq-streamer', MainHandler),
    ('/bq-task', BqHandler)
], debug=True)