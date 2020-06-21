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

/** Contains Google Ads API configurations. */
public class GoogleAdsConfiguration {

  private String clientId;
  private String clientSecret;
  private String refreshToken;
  private String developerToken;
  private long loginCustomerId;

  public GoogleAdsConfiguration(
      String clientId,
      String clientSecret,
      String refreshToken,
      String developerToken,
      long loginCustomerId) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshToken = refreshToken;
    this.developerToken = developerToken;
    this.loginCustomerId = loginCustomerId;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getRefreshToken() {
    return refreshToken;
  }

  public String getDeveloperToken() {
    return developerToken;
  }

  public long getLoginCustomerId() {
    return loginCustomerId;
  }
}
