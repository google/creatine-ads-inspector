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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.protobuf.Descriptors;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;

/** Wraps biquery calls to the API. */
public class BigQueryUtils {

  private static final Logger logger = Logger.getLogger(BigQueryUtils.class.getName());

  private BigQuery bigquery;
  private Configuration configuration;

  public BigQueryUtils(BigQuery bigquery, Configuration conf) {
    this.bigquery = bigquery;
    this.configuration = conf;
  }

  /**
   * This is (quite a heavy) method that handles all mappings to transform almost ANY
   * FieldDescriptor of the GoogleAds API and transform it in a object that BigQuery is able to use.
   *
   * <p>The complexity of this method comes from very specific issues and incomplete areas of the
   * API that should be covered when it will officially comes out in Q1-2019.
   *
   * @param fieldDescriptors extracted from an AdWord Object Protobuf
   * @return Field[] fields that are understandable by BigQuery
   */
  public Field[] createFields(List<Descriptors.FieldDescriptor> fieldDescriptors) {
    // Create field list
    Field[] fields = new Field[fieldDescriptors.size()];

    // For each field descriptor we store the type except for ENUM and MESSAGE which we will cast to
    // STRING
    for (ListIterator<Descriptors.FieldDescriptor> it = fieldDescriptors.listIterator();
        it.hasNext(); ) {
      Descriptors.FieldDescriptor fd = it.next();
      LegacySQLTypeName type = null;
      Field[] subType = new Field[0];

      if (fd.getFullName().contains("idOrB")) {
        System.out.println("here");
      }
      switch (fd.getType().name()) {
        case "ENUM":
          type = LegacySQLTypeName.STRING;
          break;
        case "MESSAGE":
          if (fd.getMessageType().getName().equals("StringValue")) {
            type = LegacySQLTypeName.STRING;
          } else if (fd.getMessageType().getName().equals("Int64Value")
              || fd.getMessageType().getName().equals("Int32Value")) {
            type = LegacySQLTypeName.INTEGER;
          } else if (fd.getMessageType().getName().equals("BoolValue")) {
            type = LegacySQLTypeName.BOOLEAN;
          } else {
            if (fd.getName().equals("http_code")) {
              type = LegacySQLTypeName.INTEGER;
            } else if (fd.getName().equals("texts")) {
              type = LegacySQLTypeName.STRING;
            } else if (!fd.getMessageType().getName().equals("HotelAdInfo")
                && !fd.getMessageType().getName().equals("ShoppingSmartAdInfo")
                && !fd.getMessageType().getName().equals("ShoppingProductAdInfo")
                && !fd.getName().equals("http_code")) {
              type = LegacySQLTypeName.RECORD;
              if (fd.getMessageType().getFields().size() > 0) {
                subType = createFields(fd.getMessageType().getFields());
              } else {
                subType = new Field[1];
                subType[0] = Field.newBuilder(fd.getName(), LegacySQLTypeName.STRING).build();
              }
            } else {
              type = LegacySQLTypeName.STRING;
            }
          }
          break;
        case "INT32":
          type = LegacySQLTypeName.INTEGER;
          break;
        case "DOUBLE":
          type = LegacySQLTypeName.FLOAT;
          break;
        default:
          type = LegacySQLTypeName.valueOf(fd.getType().name());
          break;
      }

      Field.Builder field = Field.newBuilder(fd.getName(), type, subType);
      if (type.equals(LegacySQLTypeName.RECORD)
          || fd.toProto().getLabel().name().equals("LABEL_REPEATED")) {
        field.setMode(Field.Mode.REPEATED);
      }

      fields[it.previousIndex()] = field.build();
    }

    return fields;
  }

  /**
   * This method creates a job on BigQuery that will use a JSON file stored on Google Cloud Storage
   * and load it in a specified table
   *
   * @param config the Configuration to get the BigQuery dataset name and Google Cloud Storage
   *     Bucket Name
   * @param tableName the name of the table on which the data will be loaded on BigQuery
   * @param fields the Fields defining the structure of the BigQuery table
   * @param blobName the name of the blob on BigQuery
   * @return the number of rows that were created by the job
   */
  public Long loadJSONToBigQuery(
      Configuration config, String tableName, Field[] fields, String blobName) {
    Schema schema = Schema.of(fields);
    TableId tableId = TableId.of(config.getBqDataSet(), tableName);
    String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    LoadJobConfiguration jobConfiguration =
        LoadJobConfiguration.builder(
                tableId, "gs://" + config.getBucketName() + "/" + today + "/" + blobName)
            .setFormatOptions(FormatOptions.json())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setSchema(schema)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();
    try {
      // Load the table
      JobInfo ji = JobInfo.of(jobConfiguration);
      Job loadJob = bigquery.create(ji);

      loadJob.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return ((StandardTableDefinition) bigquery.getTable(tableId).getDefinition()).getNumRows();
  }

  /**
   * This method deletes a table from BigQuery
   *
   * @param config the Configuration to get the BigQuery dataset name and Google Cloud Storage
   *     Bucket Name
   * @param tableName the name of the table which will be deleted
   */
  public boolean deleteTable(Configuration config, String tableName) {
    logger.info("Deleting table " + tableName);
    TableId tableId = TableId.of(config.getBqDataSet(), tableName);
    return bigquery.delete(tableId);
  }
}
