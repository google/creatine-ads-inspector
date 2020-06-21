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
import com.google.ads.googleads.v3.resources.AdGroupAd;
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

/** Wraps API calls to retrieve AdGroupAds. */
public class RetrieveAd {

  private static final Logger logger = Logger.getLogger(RetrieveAd.class.getName());

  private GoogleAdsClient googleAdsClient;

  private final Integer MAX_PER_LOOP = 50000;

  public RetrieveAd(GoogleAdsClient googleAdsClient) {
    this.googleAdsClient = googleAdsClient;
  }

  /**
   * Retrieves AdGroupAds from a customer ID.
   *
   * @param customerId a customer ID.
   * @param gcStorage the GoogleCloudStorageUtils object.
   * @param blobBaseName the basename to be used to create the blobs in Cloud Storage.
   * @return JsonArray containing the blobs' path to be uploaded to BigQuery.
   */
  public List<String> getAdsFromCustomerId(
      String customerId, GoogleCloudStorageUtils gcStorage, String blobBaseName) {
    List<AdGroupAd> itemsList = new ArrayList<AdGroupAd>();
    List<String> gcsBlobs = new ArrayList<String>();

    logger.info("[RetrieveAd] Get Google Ads AdGroupAds");
    logger.info("[RetrieveAd] Retrieving AdGroupsAds for customer " + customerId);
    SearchGoogleAdsRequest requestAdGroupAds =
        SearchGoogleAdsRequest.newBuilder()
            .setCustomerId(customerId)
            .setQuery(
                "SELECT "
                    + "         ad_group_ad.ad.id,"
                    + "         ad_group_ad.resource_name,"
                    + "         ad_group_ad.status,"
                    + "         ad_group_ad.ad_group,"
                    + "         ad_group_ad.resource_name,"
                    + "         ad_group_ad.policy_summary"
                    + " FROM ad_group_ad ")
            .build();

    try (GoogleAdsServiceClient googleAdsServiceClient =
        googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
      SearchPagedResponse searchPagedResponse = googleAdsServiceClient.search(requestAdGroupAds);
      Integer i = 0;
      Integer loop = 0;
      for (GoogleAdsRow googleAdsRow : searchPagedResponse.iterateAll()) {
        AdGroupAd adGroupAd = googleAdsRow.getAdGroupAd();
        itemsList.add(adGroupAd);
        i++;
        if (i % MAX_PER_LOOP == 0) {
          logger.info("[RetrieveAd] Saving checkpoint at " + i + " position");
          String blobName = blobBaseName.replace("(index)", String.valueOf(loop));
          List<String> itemsToStore = convertToNDJson(itemsList);
          gcsBlobs.addAll(gcStorage.writeToBucket(itemsToStore, blobName));
          itemsList = new ArrayList<AdGroupAd>();
          loop++;
        }
      }
      if (itemsList.size() > 0) {
        logger.info("[RetrieveAd] Saving last items");
        String blobName = blobBaseName.replace("(index)", String.valueOf(loop));
        List<String> itemsToStore = convertToNDJson(itemsList);
        gcsBlobs.addAll(gcStorage.writeToBucket(itemsToStore, blobName));
        itemsList = new ArrayList<AdGroupAd>();
      }
    } catch (Exception e) {
      System.out.println("[RetrieveAd] Could not get Ads from CID " + customerId);
      logger.severe("[RetrieveAd] Could not get Ads from CID " + customerId);
      logger.log(Level.SEVERE, e.getMessage(), e);
    }

    return gcsBlobs;
  }

  /**
   * Transforms a List of AdGroupAds into a corresponding ArrayList containing each AdGroupAd as a
   * JSON.
   *
   * @param adsList a list of AdGroupAds (it will be emptied by this function)
   * @return JsonArray containing the AdGroupAds
   */
  public List<String> convertToNDJson(List<AdGroupAd> adsList) {
    AdGroupAd ad;
    List<String> ads = new ArrayList<>();
    JsonFormat.Printer jsonAd =
        JsonFormat.printer().omittingInsignificantWhitespace().preservingProtoFieldNames();
    for (ListIterator<AdGroupAd> it = adsList.listIterator(); it.hasNext(); ) {
      try {
        ad = it.next();
        ads.add(jsonAd.print(ad));
      } catch (InvalidProtocolBufferException e) {
        logger.severe("[RetrieveAdGroups] Could not parse Ad");
        e.printStackTrace();
      }
    }
    return ads;
  }
}
