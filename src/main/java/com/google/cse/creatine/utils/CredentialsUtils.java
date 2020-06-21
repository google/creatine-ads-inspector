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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.KeyFactory;
import com.google.cse.creatine.CreatineStarter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.logging.Logger;

/** Helper class to build oauth2 credentials. */
public class CredentialsUtils {

  private static final Logger logger = Logger.getLogger(CreatineStarter.class.getName());

  /** Builds GoogleCredentials object for the Default Service Account. */
  public static GoogleCredentials retrieveDefaultServiceAccountGCreds() throws IOException {
    return GoogleCredentials.getApplicationDefault();
  }

  /**
   * Gets Google Ads configuration to call APIs from DataStore.
   *
   * @param entityId, id of the datastore entity containing the configuration
   * @return an instance of AwConfiguration containing the parsed configuration
   */
  public GoogleAdsConfiguration getGoogleAdsConfig(Long entityId) {

    // Authorized Datastore service
    Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
    KeyFactory keyFactory = datastore.newKeyFactory().setKind("googleadsconfig");
    Entity entity = datastore.get(keyFactory.newKey(entityId));

    if (entity != null) {
      String credentialsAsString = (String) entity.getValue("creds").get();
      JsonObject creds = new JsonParser().parse(credentialsAsString).getAsJsonObject();
      Gson gson = new Gson();

      return new GoogleAdsConfiguration(
          gson.fromJson(creds.get("api.googleads.clientId"), String.class),
          gson.fromJson(creds.get("api.googleads.clientSecret"), String.class),
          gson.fromJson(creds.get("api.googleads.refreshToken"), String.class),
          gson.fromJson(creds.get("api.googleads.developerToken"), String.class),
          gson.fromJson(creds.get("api.googleads.loginCustomerId"), Long.class));
    }

    return null;
  }
}
