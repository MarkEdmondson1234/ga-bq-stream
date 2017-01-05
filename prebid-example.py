import webapp2, json, logging
from oauth2client.appengine import AppAssertionCredentials
from httplib2 import Http
from apiclient import discovery
import time, uuid
from google.appengine.api import memcache, taskqueue
from datetime import date, timedelta

class MainHandler(webapp2.RequestHandler):
	def post(self):
		self.response.headers.add_header("Access-Control-Allow-Origin", "*")
		b = self.request.get("bq")		
		task = taskqueue.add(url='/bq-task', params={'bq': b})

class CreateTable(webapp2.RequestHandler):
	def get(self):
		bq = memcache.get("bq")

		#bq = None
		if bq is None:
			bq = discovery.build('bigquery', 'v2', http = Http())
			memcache.set("bq",bq)

		http_auth = memcache.get("http_auth")
		logging.debug(http_auth)
		http_auth = None
		if http_auth is None:
			credentials = AppAssertionCredentials('https://www.googleapis.com/auth/bigquery')
			http_auth = credentials.authorize(Http())
			memcache.set("http_auth", http_auth)

		projectId = ''
		datasetId = ''
		tableId = ''

		tomorrow = (date.today() + timedelta(days=1)).strftime("%Y%m%d")

		datedTable = "%s_%s"%(tableId, tomorrow)

		table_body = {
				 "kind": "bigquery#table",
				 "tableReference": {
				    "projectId": projectId,
				    "datasetId": datasetId,
				    "tableId": datedTable
				  },
				
				 "schema": {
				  "fields": [
				   {
				    "name": "timestamp",
				    "type": "INTEGER",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "bidWinner",
				    "type": "BOOLEAN",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "adId",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "adUnitCode",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "bidder",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "bidderCode",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "cpm",
				    "type": "FLOAT",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "height",
				    "type": "INTEGER",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "pbAg",
				    "type": "FLOAT",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "pbDg",
				    "type": "FLOAT",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "pbHg",
				    "type": "FLOAT",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "pbLg",
				    "type": "FLOAT",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "pbMg",
				    "type": "FLOAT",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "requestId",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "requestTimestamp",
				    "type": "INTEGER",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "responseTimestamp",
				    "type": "INTEGER",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "statusMessage",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "timeToRespond",
				    "type": "INTEGER",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "width",
				    "type": "INTEGER",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "clientId",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "page",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   },
				   {
				    "name": "hostname",
				    "type": "STRING",
				    "mode": "NULLABLE"
				   }
				  ]
				 }
				}

		request = bq.tables().insert(projectId=projectId, datasetId=datasetId, body=table_body)
		response = request.execute(http=http_auth)

		


class BqHandler(webapp2.RequestHandler):
	def post(self):
		b = self.request.get("bq")
		b = json.loads(b)


		if len(b) > 0:
			projectId = ''
			datasetId = ''
			tableId = ''

			today = date.today().strftime("%Y%m%d")

			tableId = "%s_%s"%(tableId, today)
			
			bq = memcache.get("bq")

			#bq = None
			if bq is None:
				bq = discovery.build('bigquery', 'v2', http = Http())
				memcache.set("bq",bq)

			http_auth = memcache.get("http_auth")
			logging.debug(http_auth)
			http_auth = None
			if http_auth is None:
				credentials = AppAssertionCredentials('https://www.googleapis.com/auth/bigquery.insertdata')
				http_auth = credentials.authorize(Http())
				memcache.set("http_auth", http_auth)


			rows = []
			for row in b:
				row['timestamp'] = int(time.time())
				rows.append({
				      "insertId": str(uuid.uuid4()),
				      "json": row
				    })

			table_data_insert_all_request_body = {
				  "kind": "bigquery#tableDataInsertAllRequest",
				  "skipInvalidRows": True,
				  "ignoreUnknownValues": True,
				  #"templateSuffix": string,
				  "rows": rows
				}


			bqrequest = bq.tabledata().insertAll(projectId=projectId, datasetId=datasetId, tableId=tableId, body=table_data_insert_all_request_body)
			bqresponse = bqrequest.execute(http=http_auth)



app = webapp2.WSGIApplication([
    ('/bq-streamer', MainHandler),
    ('/bq-task', BqHandler),
    ('/create-table', CreateTable)
], debug=True)