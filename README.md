# Stream Google Analytics data to BigQuery via Google App Engine

This is a Google App Engine application for streaming JSON into BigQuery

When you request the URL `/bq-streamer` with the parameter `?bq={'json':'example'}` then it will start a new task to put that JSON into a partitioned table in BigQuery.

The task is activated via a POST request to `/bq-task` with the same JSON as passed to `/bq-streamer`

See the [Google App Engine Samples](https://github.com/MarkEdmondson1234/python-docs-samples/tree/master/appengine/standard) for how to deploy.

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
2. Many samples require extra libraries to be installed. If there is a `requirements.txt`, you will need to install the dependencies with [`pip`](pip.readthedocs.org).

        pip install -t lib -r requirements.txt

3. Browse to the directory holding the `app.yaml` then issue:

        gcloud app deploy —project [YOUR_PROJECT_ID]

Optional flags:

* Include the `--project` flag to specify an alternate Cloud Platform Console project ID to what you initialized as the default in the gcloud tool. Example: `--project [YOUR_PROJECT_ID]`
* Include the -v flag to specify a version ID, otherwise one is generated for you. Example: `-v [YOUR_VERSION_ID]`

4. Visit `https://your-app-id.appost.com` to view your application.

## Additional resources

For more information on App Engine:

> https://cloud.google.com/appengine

For more information on Python on App Engine:

> https://cloud.google.com/appengine/docs/python