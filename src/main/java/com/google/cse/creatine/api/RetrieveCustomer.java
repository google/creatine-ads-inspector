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
import com.google.ads.googleads.v3.resources.CustomerClientLink;
import com.google.ads.googleads.v3.services.GoogleAdsRow;
import com.google.ads.googleads.v3.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v3.services.GoogleAdsServiceClient.SearchPagedResponse;
import com.google.ads.googleads.v3.services.SearchGoogleAdsRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;

/** Wraps API calls to retrieve AdWords Customers */
public class RetrieveCustomer {

  private static final Logger logger = Logger.getLogger(RetrieveCustomer.class.getName());

  private GoogleAdsClient googleAdsClient;

  public RetrieveCustomer(GoogleAdsClient googleAdsClient) {
    this.googleAdsClient = googleAdsClient;
  }

  /**
   * Scans all CustomerClientLink under an MCC ID. We use CustomerClientLink and not Customer since
   * it is easier to get through the API and already provides all the information we need.
   *
   * NB : This method won't work if you try to scan accounts under a test MCC account. The
   * minimum account type access is "Basic"
   *
   * @param customerId main MCC id
   * @return JsonArray containing the locations
   */
  public List<CustomerClientLink> getSubAccountsFromMCCId(String customerId) {

    List<CustomerClientLink> accountList = new ArrayList<CustomerClientLink>();

    logger.info("[RetrieveCustomer] Get AW Sub-accounts");

    SearchGoogleAdsRequest requestAdGroups =
        SearchGoogleAdsRequest.newBuilder()
            .setCustomerId(customerId)
            .setQuery(
                "SELECT customer_client_link.client_customer, "
                    + "         customer_client_link.resource_name, "
                    + "         customer_client_link.manager_link_id, "
                    + "         customer_client_link.status "
                    + " FROM customer_client_link ")
            .build();

    try (GoogleAdsServiceClient googleAdsServiceClient =
        googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
      SearchPagedResponse searchPagedResponse = googleAdsServiceClient.search(requestAdGroups);
      for (GoogleAdsRow googleAdsRow : searchPagedResponse.iterateAll()) {
        CustomerClientLink customerClientLink = googleAdsRow.getCustomerClientLink();
        accountList.add(customerClientLink);
      }
    } catch (Exception e) {
      logger.severe("[RetrieveCustomer] Could not get Customer from MCC");
    }

    return accountList;
  }

  /**
   * Transforms an List of Customers into a corresponding ArrayList containing each Customer as a
   * JSON.
   *
   * @param customerList a list of CustomerClientLink
   * @return JsonArray containing the AdGroups
   */
  public List<String> convertToNDJson(List<CustomerClientLink> customerList) {
    CustomerClientLink campaign;
    List<String> customers = new ArrayList<>();
    JsonFormat.Printer printer =
        JsonFormat.printer().omittingInsignificantWhitespace().preservingProtoFieldNames();
    for (ListIterator<CustomerClientLink> it = customerList.listIterator(); it.hasNext(); ) {
      try {
        campaign = it.next();
        customers.add(printer.print(campaign));
      } catch (InvalidProtocolBufferException e) {
        logger.severe("[RetrieveCustomer] Could not parse Campaign");
        e.printStackTrace();
      }
    }
    return customers;
  }
}
