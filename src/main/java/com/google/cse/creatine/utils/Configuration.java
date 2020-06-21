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

package com.google.cse.creatine.utils;

/** Helper class to pass BigQuery Configuration. */
public class Configuration {

  private String bqDataSet = "";
  private String cloudProject = "";
  private String bucketName = "";
  private String accountTable = "";
  private String accountLabelTable = "";
  private String campaignTable = "";
  private String campaignBudgetTable = "";
  private String adGroupTable = "";
  private String adGroupAdTable = "";
  private String googleAdsMccId = "";

  public Configuration(
      String dataSet,
      String cloudProject,
      String bucketName,
      String accountTable,
      String accountLabelTable,
      String campaignTable,
      String campaignBudgetTable,
      String adGroupTable,
      String adGroupAdTable,
      String adwordsMccId) {
    this.bqDataSet = dataSet;
    this.cloudProject = cloudProject;
    this.bucketName = bucketName;
    this.accountTable = accountTable;
    this.accountLabelTable = accountLabelTable;
    this.campaignTable = campaignTable;
    this.campaignBudgetTable = campaignBudgetTable;
    this.adGroupAdTable = adGroupAdTable;
    this.adGroupTable = adGroupTable;
    this.googleAdsMccId = adwordsMccId;
  }

  public String getBqDataSet() {
    return bqDataSet;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getCloudProject() {
    return cloudProject;
  }

  public String getAccountTable() {
    return accountTable;
  }

  public String getAccountLabelTable() {
    return accountLabelTable;
  }

  public String getCampaignTable() {
    return campaignTable;
  }

  public String getCampaignBudgetTable() {
    return campaignBudgetTable;
  }

  public String getAdGroupTable() {
    return adGroupTable;
  }

  public String getAdGroupAdTable() {
    return adGroupAdTable;
  }

  public String getGoogleAdsMccId() {
    return googleAdsMccId;
  }

  public String toString() {
    return String.format(
        "BigQuery Dataset : %s \n"
            + "Bucket Name : %s \n"
            + "Cloud Project : %s\n"
            + "Google Ads MCC Id : %s",
        bqDataSet, bucketName, cloudProject, googleAdsMccId);
  }
}
