# Stream Google Analytics data to BigQuery via Google App Engine

This is a Google App Engine application for streaming JSON into BigQuery

When you request the URL `/bq-streamer` with the parameter `?bq={'json':'example'}` then it will start a new task to put that JSON into a partitioned table in BigQuery.

The task is activated via a POST request to `/bq-task` with the same JSON as passed to `/bq-streamer`

