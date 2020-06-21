# ⚡ Creatine - Ads Inspector

**This is not an officially supported Google product.**

Creatine is a scalable solution that runs on Google Cloud Platform, uses Google 
Ads API to take snapshots of Google Ads account hierarchy, stores them on 
BigQuery and creates a report to monitor Ads disapprovals over time.

## Intended audience

Java Developer and Google Cloud administrator

> It is assumed that the user is familiar with Google Ads API and Google Cloud
> platform

## Prerequisites

1) Your Google Ads API Client ID, Client Secret, Developer Token, Refresh Token and Main
MCC ID (No test account, only Basic or Standard access) are required.

For more information on how to get those credentials, please see the Ads Dev
Guide : https://developers.google.com/adwords/api/docs/guides/first-api-call

For more information on Access levels, please see the documentation available :
https://developers.google.com/adwords/api/docs/access-levels

This solution leverages:

*   AppEngine, DataStore, Cloud Storage and BigQuery
    [APIs must be enabled](https://console.cloud.google.com/apis/library)
*   [Google Cloud SDK](https://cloud.google.com/sdk/) (gcloud)
*   [Maven command line tool (mvn)](https://maven.apache.org/download.cgi) 3.5+
*   [Java 8](https://www.java.com/fr/download/)

## Quick start

### Deploy the solution:

1.  Create a
    [Google Cloud project](https://console.cloud.google.com/projectcreate)
    via the Cloud console or with the following script

    ```shell
     export PROJECT_ID="ads-creatine-${RANDOM:0:4}"
     gcloud projects create $PROJECT_ID --enable-cloud-apis --set-as-default
    ```

    Take note of the PROJECT_ID value, as we it will be used in next steps.

2.  Enable Google,
    [Google Ads](https://console.cloud.google.com/apis/library/googleads.googleapis.com),
    [Datastore](https://console.cloud.google.com/apis/library/datastore.googleapis.com),
    [BigQuery](https://console.cloud.google.com/apis/library/bigquery-json.googleapis.com),
    [Compute Engine](https://console.cloud.google.com/apis/api/compute.googleapis.com)
    and
    [Google Cloud Storage](https://console.cloud.google.com/apis/library/storage-api.googleapis.com)
    APIs via the cloud console or using a script

    ```shell
       gcloud services enable googleads.googleapis.com \
       datastore.googleapis.com \
       bigquery-json.googleapis.com \
       compute.googleapis.com \
       storage-api.googleapis.com \
       --async
    ```

3.  Download source code on your worksation (not a cloud VM), you can get the code from
    [github](https://github.com/google/creatine-ads-inpector)

    ```shell
       git clone https://github.com/google/creatine-google-ads-inspector
    ```

4.  `cd creatine/`

5.  run `gcloud init` to ensure that your are on the right Cloud project

6.  Open `pom.xml` and update the `GCP_PROJECT_ID` placeholder with the ID of 
    the project you created (under element project > build > plugins > plugin)

7.  Create a new
    [datastore entity](https://cloud.google.com/datastore/docs/quickstart#store_data)
    via the Cloud Console.

    (If requested, select "Cloud Datastore" for data storage, and a convenient
    location.)

    Select the "query by kind" tab :

    *   namespace: [default]
    *   kind: googleadsconfig
    *   key identifier: Numeric ID (auto-generated)

8.  Edit `googleadsconfig` entity and create a new property

    *   Name: creds
    *   Type: text
    *   Value:

    ```json
    { 
      "api.googleads.clientId":"YOUR_CLIENT_ID",
      "api.googleads.clientSecret":"YOUR_CLIENT_SECRET",
      "api.googleads.refreshToken":"YOUR_REFRESH_CONTENT",
      "api.googleads.loginCustomerId":"YOUR_LOGIN_CUSTOMER_ID",
      "api.googleads.developerToken":"YOUR_DEVELOPER_CONTENT" 
    }
    ```

9.  [Create a BigQuery Dataset](https://cloud.google.com/bigquery/docs/datasets#creating_a_dataset)

    or using the following command line

    ```shell
    bq --location=EU mk --dataset "$PROJECT_ID":name_of_your_dataset
    ```

10. Open `application.properties` file under `src/main/resources`

11. Update the properties surrounded with <>

    ```json
      bqDataSet=<BIGQUERY_DATASET_NAME>
      cloudProject=<PROJECT_ID>
      entityId=<DATASTORE_ENTITY_ID>
      gcsBucket=<GOOGLE_CLOUD_STORAGE_BUCKET_NAME>
      googleAdsMccId=<MAIN_MCC_ID> (same as the one from DataStore)
    ```

    eg:<br>

    ```json
      bqDataSet=creatine
      cloudProject=ads-creatine-2162
      entityId=5629499534213120
      gcsBucket=creatine
      googleAdsMccId=8053850078
    ```

12. run `mvn package appengine:deploy` to deploy the app

13. run `gcloud app deploy cron.yaml` to schedule the cron job

14. Verify that the configuration is up and running:

    *   Go to the
        [cron job console](https://console.cloud.google.com/appengine/cronjobs)
    *   Click Run Now
    *   Wait a few minutes for the completion of the task and check that your BigQuery dataset was updated.


## Licensing

Terms of the release - Copyright 2018 Google LLC. Licensed under the Apache
License, Version 2.0.
