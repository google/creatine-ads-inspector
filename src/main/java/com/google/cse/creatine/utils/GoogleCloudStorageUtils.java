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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.storage.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/** Wraps biquery calls to the Google Cloud Storage API. */
public class GoogleCloudStorageUtils {

  private static final Logger logger = Logger.getLogger(GoogleCloudStorageUtils.class.getName());

  private final Storage storage;
  private Bucket bucket;

  public GoogleCloudStorageUtils(Storage storage, Configuration conf) {
    this.storage = storage;
  }

  /**
   * Created a bucket if it doesn't exist
   *
   * @param bucketName name of the bucket that will be created
   * @return the bucket that was created
   */
  public Bucket createBucket(String bucketName) {
    bucket = storage.get(bucketName, Storage.BucketGetOption.fields());
    if (bucket == null) {
      bucket = storage.create(BucketInfo.of(bucketName));
    }
    return bucket;
  }

  /**
   * This method writes the content in the dataList to the file with the blobName into Google Cloud
   * Storage
   *
   * @param dataList the list containing the data
   * @param blobName the name of the blob where the data should be written
   * @return a String containing the Google Cloud Storage link to the blob
   */
  public List<String> writeToBucket(List<String> dataList, String blobName) {
    BlobId blobId;
    BlobInfo blobInfo;
    Integer chunkSize = 20000;

    StringBuilder sb = new StringBuilder();

    // Splitting mechanism to avoid OutOfMemory errors
    List<String> elementsAdded = new ArrayList<>();
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    for (int i = dataList.size(); i > 0; i--) {
      sb.append(dataList.get(i - 1));
      sb.append("\n");
      dataList.remove(i - 1);
      // Every "{chunkSize}" lines, we send a new bloc to GCS or at the end of the list
      if (i % chunkSize == 0 || i - 1 == 0) {
        int chunkNumber = (int) Math.round(Math.floor(i / chunkSize) + 1);
        blobId =
            BlobId.of(
                bucket.getName(),
                today + "/" + blobName.replace("(chunk)", String.valueOf(chunkNumber)));
        elementsAdded.add(blobName.replace("(chunk)", String.valueOf(chunkNumber)));
        blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build();
        storage.create(blobInfo, sb.toString().getBytes(UTF_8));
        // Empty the StringBuffer
        sb.delete(0, sb.length());
      }
    }

    // Blob blob = storage.create(blobInfo, sb.toString().getBytes(UTF_8));
    return elementsAdded;
  }
}
