# Stream Google Analytics data to BigQuery via Google App Engine

This is a Google App Engine application for streaming JSON into BigQuery

When you request the URL `/bq-streamer` with the parameter `?bq={'json':'example'}` then it will start a new task to put that JSON into a partitioned table in BigQuery.

The task is activated via a POST request to `/bq-task` with the same JSON as passed to `/bq-streamer`

See the [Google App Engine Samples](https://github.com/MarkEdmondson1234/python-docs-samples/tree/master/appengine/standard) for how to deploy.

## Setup

1. Create a dataset and date partitioned BigQuery table to receive the hits.
* Create empty table > set table name > add schema > Options: Partitioning to "DAY"
2. Add any other fields to the table that you wish to send in, the script by default also adds `ts` as a STRING that is a UNIX timestamp so add that too. Any unset fields won't be seen by default.
3. Edit the `app.yaml` field `env_variables` to your BigQuery details:

Example:

```
runtime: python27
api_version: 1
threadsafe: yes

handlers:
- url: .*
  script: main.app

#[START env]
env_variables:
  DATASET_ID: tests
  TABLE_ID: realtime
#[END env]
```

3. Deploy the app (see below)
4. Call the `https://your-app-id.appost.com/bq-streamer?bq={"this_is_json":"yes"}`  to add the fields to your BigQuery table.
5. The data won't appear in the table preview yet but you can query the table via something like `SELECT * FROM dataset.tableID` to see the hits. Turn off ÙSE CACHED RESULTS`
6. View the logs for any errors `https://console.cloud.google.com/logs/viewer`

 
## Running the samples locally

1. Download the [Google App Engine Python SDK](https://cloud.google.com/appengine/downloads) for your platform.
2. Many samples require extra libraries to be installed. If there is a `requirements.txt`, you will need to install the dependencies with [`pip`](pip.readthedocs.org).

        pip install -t lib -r requirements.txt

3. Use `dev_appserver.py` to run the sample:

        dev_appserver.py app.yaml

4. Visit `http://localhost:8080` to view your application.

Some samples may require additional setup. Refer to individual sample READMEs.

## Deploying the samples

1. Download the [Google App Engine Python SDK](https://cloud.google.com/appengine/downloads) for your platform.
2. Open terminal then browse to the folder containing `app.yaml`
3. The app requires extra libraries to be installed. You need to install the dependencies with [`pip`](pip.readthedocs.org).

This installs the libraries to a new folder `lib` in the app directory.  It most likely won't need to add anything.

        pip install -t lib -r requirements.txt

4. Deploy via:

        gcloud app deploy --project [YOUR_PROJECT_ID]

Optional flags:

* Include the `--project` flag to specify an alternate Cloud Platform Console project ID to what you initialized as the default in the gcloud tool. Example: `--project [YOUR_PROJECT_ID]`
* Include the -v flag to specify a version ID, otherwise one is generated for you. Example: `-v [YOUR_VERSION_ID]`

4. Visit `https://your-app-id.appost.com` to view your application.

## Additional resources

For more information on App Engine:

> https://cloud.google.com/appengine

For more information on Python on App Engine:

> https://cloud.google.com/appengine/docs/python