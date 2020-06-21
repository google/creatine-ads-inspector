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
import com.google.ads.googleads.v3.resources.AdGroup;
import com.google.ads.googleads.v3.services.GoogleAdsRow;
import com.google.ads.googleads.v3.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v3.services.GoogleAdsServiceClient.SearchPagedResponse;
import com.google.ads.googleads.v3.services.SearchGoogleAdsRequest;
import com.google.cse.creatine.utils.GoogleCloudStorageUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Wraps API calls to retrieve AdWords AdGroups */
public class RetrieveAdGroup {

  private static final Logger logger = Logger.getLogger(RetrieveAdGroup.class.getName());

  private GoogleAdsClient googleAdsClient;

  private final Integer MAX_PER_LOOP = 20000;

  public RetrieveAdGroup(GoogleAdsClient googleAdsClient) {
    this.googleAdsClient = googleAdsClient;
  }

  /**
   * Retrieves AdGroups from a customer ID.
   *
   * @param customerId a customer ID.
   * @param gcStorage the GoogleCloudStorageUtils object.
   * @param blobBaseName the basename to be used to create the blobs in Cloud Storage.
   * @return JsonArray containing the blobs' path to be uploaded to BigQuery.
   */
  public List<String> getAdGroupsFromCustomerId(
      String customerId, GoogleCloudStorageUtils gcStorage, String blobBaseName) {
    List<AdGroup> itemsList = new ArrayList<AdGroup>();
    List<String> gcsBlobs = new ArrayList<String>();

    logger.info("[RetrieveAdGroups] Get Google Ads AdGroups");
    logger.info("[RetrieveAdGroups] Retrieving AdGroups for customer " + customerId);
    SearchGoogleAdsRequest requestAdGroups =
        SearchGoogleAdsRequest.newBuilder()
            .setCustomerId(customerId)
            .setQuery(
                "SELECT "
                    + "    ad_group.resource_name,"
                    + "    ad_group.id,"
                    + "    ad_group.name,"
                    + "    ad_group.status,"
                    + "    ad_group.type, "
                    + "    ad_group.ad_rotation_mode, "
                    + "    ad_group.tracking_url_template, "
                    + "    ad_group.url_custom_parameters, "
                    + "    ad_group.campaign,"
                    + "    ad_group.cpc_bid_micros, "
                    + "    ad_group.cpm_bid_micros, "
                    + "    ad_group.cpv_bid_micros, "
                    + "    ad_group.percent_cpc_bid_micros "
                    + " FROM ad_group ")
            .build();

    try (GoogleAdsServiceClient googleAdsServiceClient =
        googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
      SearchPagedResponse searchPagedResponse = googleAdsServiceClient.search(requestAdGroups);
      Integer i = 0;
      Integer loop = 0;
      for (GoogleAdsRow googleAdsRow : searchPagedResponse.iterateAll()) {
        AdGroup adGroup = googleAdsRow.getAdGroup();
        itemsList.add(adGroup);
        i++;
        if (i % MAX_PER_LOOP == 0) {
          logger.info("[RetrieveAdGroups] Saving checkpoint at " + i + " position");
          String blobName = blobBaseName.replace("(index)", String.valueOf(loop));
          List<String> itemsToStore = convertToNDJson(itemsList);
          gcsBlobs.addAll(gcStorage.writeToBucket(itemsToStore, blobName));
          itemsList = new ArrayList<AdGroup>();
          loop++;
        }
      }
      if (itemsList.size() > 0) {
        logger.info("[RetrieveAd] Saving last items");
        String blobName = blobBaseName.replace("(index)", String.valueOf(loop));
        List<String> itemsToStore = convertToNDJson(itemsList);
        gcsBlobs.addAll(gcStorage.writeToBucket(itemsToStore, blobName));
        itemsList = new ArrayList<AdGroup>();
      }
    } catch (Exception e) {
      System.out.println("[RetrieveAdGroups] Could not get Ads from CID " + customerId);
      logger.severe("[RetrieveAdGroups] Could not get Ads from CID " + customerId);
      logger.log(Level.SEVERE, e.getMessage(), e);
    }

    return gcsBlobs;
  }

  /**
   * Transforms an List of AdGroups into a corresponding ArrayList containing each AdGroup as a
   * JSON.
   *
   * @param adsList a list of AdGroups
   * @return JsonArray containing the AdGroups
   */
  public ArrayList<String> convertToNDJson(List<AdGroup> adsList) {
    AdGroup adGroup;
    ArrayList<String> ads = new ArrayList<>();
    JsonFormat.Printer jsonAd =
        JsonFormat.printer().omittingInsignificantWhitespace().preservingProtoFieldNames();
    for (ListIterator<AdGroup> it = adsList.listIterator(); it.hasNext(); ) {
      try {
        adGroup = it.next();
        ads.add(jsonAd.print(adGroup));
      } catch (InvalidProtocolBufferException e) {
        logger.severe("[RetrieveAdGroups] Could not parse Ad");
        e.printStackTrace();
      }
    }
    return ads;
  }
}
