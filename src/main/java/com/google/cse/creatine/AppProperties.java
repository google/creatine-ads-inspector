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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/** Reads application default properties in the resources/application.properties file. */
@Component
@PropertySource("classpath:application.properties")
public class AppProperties {

  @Value("${bqDataSet}")
  private String bqDataSet;

  @Value("${cloudProject}")
  private String cloudProject;

  @Value("${entityId}")
  private String entityId;

  @Value("${bqAccountTable}")
  private String bqAccountTable;

  @Value("${bqAccountLabelTable}")
  private String bqAccountLabelTable;

  @Value("${bqCampaignTable}")
  private String bqCampaignTable;

  @Value("${bqCampaignBudgetTable}")
  private String bqCampaignBudgetTable;

  @Value("${bqAdGroupAdTable}")
  private String bqAdGroupAdTable;

  @Value("${bqAdGroupTable}")
  private String bqAdGroupTable;

  @Value("${gcsBucket}")
  private String gcsBucket;

  @Value("${googleAdsMccId}")
  private String googleAdsMccId;

  public String getGoogleAdsMccId() {
    return googleAdsMccId;
  }

  public String getBqDataSet() {
    return bqDataSet;
  }

  public String getCloudProject() {
    return cloudProject;
  }

  public String getEntityId() {
    return entityId;
  }

  public String getAccountTable() {
    return bqAccountTable;
  }

  public String getAccountLabelTable() {
    return bqAccountLabelTable;
  }

  public String getCampaignTable() {
    return bqCampaignTable;
  }

  public String getCampaignBudgetTable() {
    return bqCampaignBudgetTable;
  }

  public String getAdGroupTable() {
    return bqAdGroupTable;
  }

  public String getAdGroupAdTable() {
    return bqAdGroupAdTable;
  }

  public String getBucketName() {
    return gcsBucket;
  }
}
