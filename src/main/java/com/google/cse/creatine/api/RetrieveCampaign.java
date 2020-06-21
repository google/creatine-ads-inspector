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

package com.google.cse.creatine.api;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v3.resources.Campaign;
import com.google.ads.googleads.v3.services.GoogleAdsRow;
import com.google.ads.googleads.v3.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v3.services.GoogleAdsServiceClient.SearchPagedResponse;
import com.google.ads.googleads.v3.services.SearchGoogleAdsRequest;
import com.google.cse.creatine.utils.GoogleCloudStorageUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Wraps API calls to retrieve Campaigns. */
public class RetrieveCampaign {

  private static final Logger logger = Logger.getLogger(RetrieveCampaign.class.getName());

  private GoogleAdsClient googleAdsClient;

  private final Integer MAX_PER_LOOP = 20000;

  public RetrieveCampaign(GoogleAdsClient googleAdsClient) {
    this.googleAdsClient = googleAdsClient;
  }

  /**
   * Retrieves Campaigns from a customer ID.
   *
   * @param customerId a customer ID.
   * @param gcStorage the GoogleCloudStorageUtils object.
   * @param blobBaseName the basename to be used to create the blobs in Cloud Storage.
   * @return JsonArray containing the blobs' path to be uploaded to BigQuery.
   */
  public List<String> getCampaignsFromCustomerId(
      String customerId, GoogleCloudStorageUtils gcStorage, String blobBaseName) {
    List<Campaign> itemsList = new ArrayList<Campaign>();
    List<String> gcsBlobs = new ArrayList<String>();

    logger.info("[RetrieveCampaign] Get Google Ads Campaigns");
    logger.info("[RetrieveCampaign] Retrieving Campaigns for customer " + customerId);
    SearchGoogleAdsRequest requestCampaigns =
        SearchGoogleAdsRequest.newBuilder()
            .setCustomerId(customerId)
            .setQuery(
                "SELECT "
                    + "         campaign.ad_serving_optimization_status,"
                    + "         campaign.campaign_budget,"
                    + "         campaign.end_date,"
                    + "         campaign.id, "
                    + "         campaign.name,"
                    + "         campaign.resource_name,"
                    + "         campaign.serving_status,"
                    + "         campaign.start_date,"
                    + "         campaign.status"
                    + " FROM campaign ")
            .build();

    try (GoogleAdsServiceClient googleAdsServiceClient =
        googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
      SearchPagedResponse searchPagedResponse = googleAdsServiceClient.search(requestCampaigns);
      Integer i = 0;
      Integer loop = 0;
      for (GoogleAdsRow googleAdsRow : searchPagedResponse.iterateAll()) {
        Campaign campaign = googleAdsRow.getCampaign();
        itemsList.add(campaign);
        i++;
        if (i % MAX_PER_LOOP == 0) {
          logger.info("[RetrieveCampaign] Saving checkpoint at " + i + " position");
          String blobName = blobBaseName.replace("(index)", String.valueOf(loop));
          List<String> itemsToStore = convertToNDJson(itemsList);
          gcsBlobs.addAll(gcStorage.writeToBucket(itemsToStore, blobName));
          itemsList = new ArrayList<Campaign>();
          loop++;
        }
      }
      if (itemsList.size() > 0) {
        logger.info("[RetrieveCampaign] Saving last items");
        String blobName = blobBaseName.replace("(index)", String.valueOf(loop));
        List<String> itemsToStore = convertToNDJson(itemsList);
        gcsBlobs.addAll(gcStorage.writeToBucket(itemsToStore, blobName));
        itemsList = new ArrayList<Campaign>();
      }
    } catch (Exception e) {
      System.out.println("[RetrieveCampaign] Could not get Ads from CID " + customerId);
      logger.severe("[RetrieveCampaign] Could not get Ads from CID " + customerId);
      logger.log(Level.SEVERE, e.getMessage(), e);
    }

    return gcsBlobs;
  }

  /**
   * Transforms an List of Campaigns into a corresponding ArrayList containing each Campaign as a
   * JSON.
   *
   * @param campaignList a list of Campaigns
   * @return JsonArray containing the Campaigns
   */
  public List<String> convertToNDJson(List<Campaign> campaignList) {
    Campaign campaign;
    List<String> campaigns = new ArrayList<>();
    JsonFormat.Printer printer =
        JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames();
    for (ListIterator<Campaign> it = campaignList.listIterator(); it.hasNext(); ) {
      try {
        campaign = it.next();
        campaigns.add(printer.print(campaign));
      } catch (InvalidProtocolBufferException e) {
        logger.severe("[RetrieveCampaign] Could not parse Campaign");
        e.printStackTrace();
      }
    }
    return campaigns;
  }
}
