// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cse.creatine;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v3.resources.AdGroup;
import com.google.ads.googleads.v3.resources.AdGroupAd;
import com.google.ads.googleads.v3.resources.Campaign;
import com.google.ads.googleads.v3.resources.CustomerClientLink;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.auth.Credentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.storage.StorageOptions;
import com.google.cse.creatine.api.RetrieveAd;
import com.google.cse.creatine.api.RetrieveAdGroup;
import com.google.cse.creatine.api.RetrieveCampaign;
import com.google.cse.creatine.api.RetrieveCustomer;
import com.google.cse.creatine.utils.*;
import com.google.gson.Gson;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Bootstrap the Spring-based application. */
@SpringBootApplication
@RestController
public class CreatineStarter {

  private static final Logger logger = Logger.getLogger(CreatineStarter.class.getName());

  private AppProperties properties;
  private Configuration configuration;
  private GoogleAdsClient googleAdsClient;
  private BigQueryUtils bQUtils;
  private GoogleCloudStorageUtils gcStorage;

  private Queue queue = QueueFactory.getDefaultQueue();

  private List<CustomerClientLink> customerList;

  /**
   * Launches the Spring based app retrieving the Customers / Campaigns /AdGroups / AdGroupAds from
   * Google Ads API
   */
  public static void main(String[] args) {
    SpringApplication.run(CreatineStarter.class, args);
  }

  @Autowired
  public void setGlobal(AppProperties properties) {
    this.properties = properties;
    this.configuration =
        new Configuration(
            this.properties.getBqDataSet(),
            this.properties.getCloudProject(),
            this.properties.getBucketName(),
            this.properties.getAccountTable(),
            this.properties.getAccountLabelTable(),
            this.properties.getCampaignTable(),
            this.properties.getCampaignBudgetTable(),
            this.properties.getAdGroupTable(),
            this.properties.getAdGroupAdTable(),
            this.properties.getGoogleAdsMccId());
    System.out.print(this.properties.getCloudProject());
    initCredentials();
  }

  /**
   * This method retrieves credentials from Datastore and instantiate the connection to all needed
   * Google services : - GoogleAds - BigQuery - Google Cloud Storage
   */
  private void initCredentials() {
    if (googleAdsClient == null) {
      try {
        CredentialsUtils credStorage = new CredentialsUtils();
        GoogleAdsConfiguration googleAdsConfig =
            credStorage.getGoogleAdsConfig(Long.parseLong(properties.getEntityId()));

        Credentials googleAdsCredentials =
            UserCredentials.newBuilder()
                .setClientId(googleAdsConfig.getClientId())
                .setClientSecret(googleAdsConfig.getClientSecret())
                .setRefreshToken(googleAdsConfig.getRefreshToken())
                .build();

        googleAdsClient =
            GoogleAdsClient.newBuilder()
                .setCredentials(googleAdsCredentials)
                .setDeveloperToken(googleAdsConfig.getDeveloperToken())
                .setLoginCustomerId(googleAdsConfig.getLoginCustomerId())
                // .setEnableGeneratedCatalog(true)
                .build();

        bQUtils =
            new BigQueryUtils(
                BigQueryOptions.newBuilder()
                    .setProjectId(configuration.getCloudProject())
                    .setCredentials(CredentialsUtils.retrieveDefaultServiceAccountGCreds())
                    .build()
                    .getService(),
                configuration);

        gcStorage =
            new GoogleCloudStorageUtils(
                StorageOptions.newBuilder()
                    .setProjectId(configuration.getCloudProject())
                    .setCredentials(CredentialsUtils.retrieveDefaultServiceAccountGCreds())
                    .build()
                    .getService(),
                configuration);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /** Clear all list entities to avoid memory heap errors */
  @GetMapping(value = "/v1/clear", produces = "application/json;UTF-8")
  public void clearCustomerList() {
    customerList = null;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void onStartup() {
    System.out.println("Loading Customers on Startup");
    refreshCustomers();
  }

  /**
   * Makes calls to all subsequents entities This method is a shortcut to avoid calling the four
   * different methods below making the execution on Google App Engine simpler
   *
   * <p>Those four tasks are not interchangeable. - AdGroupsAds and Campaigns needs Customers to be
   * executed first. - AdGroups needs AdgroupAds to be executed first
   */
  @GetMapping(value = "/v1/get_all", produces = "application/json;UTF-8")
  public void refreshAll() {
    queue.add(TaskOptions.Builder.withUrl("/v1/startlongprocess").method(TaskOptions.Method.GET));
  }

  /** Dummy method wrapping all calls to be executed inside a task queue */
  @GetMapping(value = "/v1/startlongprocess", produces = "application/json;UTF-8")
  public void startLongProcess() {
    clearCustomerList();
    refreshCustomers();
    refreshCampaigns();
    refreshAdGroupAds();
    refreshAdGroups();
    clearCustomerList();
  }

  /**
   * Retrieves the list of AdWords Customers, stores it on BigQuery and eventually returns the list
   * as a JSON object
   *
   * @return json object containing the Customers
   */
  @GetMapping(value = "/v1/get_customers", produces = "application/json;UTF-8")
  public String refreshCustomers() {
    logger.info("Starting getting Customers");
    String blobName = "accounts.json";
    RetrieveCustomer retrieveCustomer = new RetrieveCustomer(googleAdsClient);
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    String customerTable = configuration.getAccountTable().replace("[YYYYMMDD]", today);

    // Pulling Customers from AdWords
    customerList = retrieveCustomer.getSubAccountsFromMCCId(configuration.getGoogleAdsMccId());

    // Transform to JSON
    List<String> accountsToStore = retrieveCustomer.convertToNDJson(customerList);

    // Write to GCS
    gcStorage.createBucket(configuration.getBucketName());
    gcStorage.writeToBucket(accountsToStore, blobName);

    // Persist to BigQuery
    Field[] fields = bQUtils.createFields(CustomerClientLink.getDescriptor().getFields());
    bQUtils.loadJSONToBigQuery(configuration, customerTable, fields, blobName);

    logger.info("Finished getting Customers");
    return new Gson().toJson(customerList);
  }

  /**
   * Retrieves the list of Google Ads Campaigns, stores it on BigQuery and eventually returns the
   * list as a JSON object
   *
   * @return json object containing the Campaigns
   */
  @GetMapping(value = "/v1/get_campaigns", produces = "application/json;UTF-8")
  public String refreshCampaigns() {
    logger.info("Starting getting Campaigns");
    Pattern regexpCustomerId = Pattern.compile("^customers/(\\d+)$");
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());

    String campaignTable = configuration.getCampaignTable().replace("[YYYYMMDD]", today);

    // Pull Campaigns from Google Ads for each Customer
    if (!customerList.isEmpty()) {

      // Delete the current table
      bQUtils.deleteTable(configuration, campaignTable);

      // Make sure the bucket exists
      gcStorage.createBucket(configuration.getBucketName());

      for (CustomerClientLink account : customerList) {
        Matcher m = regexpCustomerId.matcher(account.getClientCustomer().getValue());
        if (account.getStatus().getValueDescriptor().getName().equals("ACTIVE") && m.find()) {
          String accountId = m.group(1);
          TaskOptions task =
              TaskOptions.Builder.withUrl("/v1/getCampaignsFromCustomerId")
                  .method(TaskOptions.Method.GET)
                  .param("customerID", accountId);
          queue.addAsync(task);
        }
      }
    } else {
      logger.warn("[CreatineStarter] Could not get Campaigns because Customer List was empty");
    }

    logger.info("Finished getting Campaigns");
    return "Finished getting Campaigns";
  }

  /**
   * Retrieves Campaigns from a Customer ID.
   *
   * @param customerID a Customer ID
   * @return void
   */
  @GetMapping(value = "/v1/getCampaignsFromCustomerId", produces = "application/json;UTF-8")
  public void getCampaignsFromCustomerId(@RequestParam(name = "customerID") String customerID) {

    String blobName = "campaign_(index)_(chunk).json";
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    String todaysBlob = String.format("%s_%s_%s", today, customerID, blobName);
    String campaignTable = configuration.getCampaignTable().replace("[YYYYMMDD]", today);

    // Create the schema of the table to BigQuery
    Field[] fields = bQUtils.createFields(Campaign.getDescriptor().getFields());

    RetrieveCampaign campaignsRetriever = new RetrieveCampaign(googleAdsClient);
    List<String> gcsBlobs =
        campaignsRetriever.getCampaignsFromCustomerId(customerID, gcStorage, todaysBlob);
    for (int i = 0; i < gcsBlobs.size(); i++) {
      logger.info("Uploading file " + gcsBlobs.get(i) + " to BigQuery");
      bQUtils.loadJSONToBigQuery(configuration, campaignTable, fields, gcsBlobs.get(i));
    }
  }

  /**
   * Retrieves the list of AdGroups, stores it on BigQuery and eventually returns the list as a JSON
   * object
   *
   * @return json object containing the AdGroups
   */
  @GetMapping(value = "/v1/get_adgroups", produces = "application/json;UTF-8")
  public String refreshAdGroups() {

    logger.info("Starting getting AdGroups");
    String cleanCustomerId;
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    String adGroupTable = configuration.getAdGroupTable().replace("[YYYYMMDD]", today);

    // Pull AdGroups from Google Ads for each Customer
    if (!customerList.isEmpty()) {

      // Delete the current table
      bQUtils.deleteTable(configuration, adGroupTable);

      // Make sure the bucket exists
      gcStorage.createBucket(configuration.getBucketName());

      for (CustomerClientLink ccl : customerList) {
        if (!ccl.getStatus().getValueDescriptor().getName().equals("INACTIVE")) {

          cleanCustomerId = ccl.getClientCustomer().getValue().replace("customers/", "");

          TaskOptions task =
              TaskOptions.Builder.withUrl("/v1/getAdGroupsFromCustomerId")
                  .method(TaskOptions.Method.GET)
                  .param("customerID", cleanCustomerId);
          queue.addAsync(task);
        }
      }
    } else {
      logger.warn("[CreatineStarter] Could not get AdGroups because Customer List was empty");
    }
    logger.info("Finished getting AdGroups");
    return "Finished getting AdGroups";
  }

  /**
   * Retrieves Google Ads AdGroups from a Customer ID
   *
   * @param customerID a Customer ID
   * @return void
   */
  @GetMapping(value = "/v1/getAdGroupsFromCustomerId", produces = "application/json;UTF-8")
  public void getAdGroupsFromCustomerId(@RequestParam(name = "customerID") String customerID) {

    String blobName = "ad_group_(index)_(chunk).json";
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    String todaysBlob = String.format("%s_%s_%s", today, customerID, blobName);
    String adGroupTable = configuration.getAdGroupTable().replace("[YYYYMMDD]", today);

    // Create the schema of the table to BigQuery
    Field[] fields = bQUtils.createFields(AdGroup.getDescriptor().getFields());

    RetrieveAdGroup adGroupsRetriever = new RetrieveAdGroup(googleAdsClient);
    List<String> gcsBlobs =
        adGroupsRetriever.getAdGroupsFromCustomerId(customerID, gcStorage, todaysBlob);
    for (int i = 0; i < gcsBlobs.size(); i++) {
      logger.info("Uploading file " + gcsBlobs.get(i) + " to BigQuery");
      bQUtils.loadJSONToBigQuery(configuration, adGroupTable, fields, gcsBlobs.get(i));
    }
  }

  /**
   * Retrieves the list of Google Ads (Ad Group) Ads, stores it on BigQuery and eventually returns
   * the list as a JSON object
   *
   * @return json object containing the AdGroupAds
   */
  @GetMapping(value = "/v1/get_adgroupads", produces = "application/json;UTF-8")
  public String refreshAdGroupAds() {
    logger.info("Starting getting AdGroupAds");

    String cleanCustomerId;

    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    String adGroupAdTable = configuration.getAdGroupAdTable().replace("[YYYYMMDD]", today);

    // Pull AdGroupAds from Google Ads for each Customer
    if (!customerList.isEmpty()) {

      // Delete the current table
      bQUtils.deleteTable(configuration, adGroupAdTable);

      // Make sure the bucket exists
      gcStorage.createBucket(configuration.getBucketName());

      for (CustomerClientLink ccl : customerList) {
        if (!ccl.getStatus().getValueDescriptor().getName().equals("INACTIVE")) {

          cleanCustomerId = ccl.getClientCustomer().getValue().replace("customers/", "");

          TaskOptions task =
              TaskOptions.Builder.withUrl("/v1/getAdGroupAdsFromCustomerId")
                  .method(TaskOptions.Method.GET)
                  .param("customerID", cleanCustomerId);
          queue.addAsync(task);
        }
      }
    } else {
      logger.warn("[CreatineStarter] Could not get AdGroupAds because Customer List was empty");
    }

    logger.info("Finished getting AdGroupAds");
    return "Finished getting AdGroupAds";
  }

  /**
   * Retrieves Google Ads AdGroupAds from a Customer ID
   *
   * @param customerID a Customer ID
   * @return void
   */
  @GetMapping(value = "/v1/getAdGroupAdsFromCustomerId", produces = "application/json;UTF-8")
  public void getAdGroupAdsFromCustomerId(@RequestParam(name = "customerID") String customerID) {

    String blobName = "ad_group_ad_(index)_(chunk).json";
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    String todaysBlob = String.format("%s_%s_%s", today, customerID, blobName);
    String adGroupAdTable = configuration.getAdGroupAdTable().replace("[YYYYMMDD]", today);

    // Create the schema of the table to BigQuery
    Field[] fields = bQUtils.createFields(AdGroupAd.getDescriptor().getFields());

    RetrieveAd adGroupAdsRetriever = new RetrieveAd(googleAdsClient);
    List<String> gcsBlobs =
        adGroupAdsRetriever.getAdsFromCustomerId(customerID, gcStorage, todaysBlob);
    for (int i = 0; i < gcsBlobs.size(); i++) {
      logger.info("Uploading file " + gcsBlobs.get(i) + " to BigQuery");
      bQUtils.loadJSONToBigQuery(configuration, adGroupAdTable, fields, gcsBlobs.get(i));
    }
  }
}
