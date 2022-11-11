/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.beehyv.nutch.indexwriter.elastic;

public interface BeehyvElasticConstants {
  public static final String ELASTIC_PREFIX = "elastic.";

  public static final String HOST = ELASTIC_PREFIX + "host";
  public static final String PORT = ELASTIC_PREFIX + "port";
  public static final String CLUSTER = ELASTIC_PREFIX + "cluster";
  public static final String INDEX = ELASTIC_PREFIX + "index";
  public static final String MAX_BULK_DOCS = ELASTIC_PREFIX + "max.bulk.docs";
  public static final String MAX_BULK_LENGTH = ELASTIC_PREFIX + "max.bulk.size";

  public static final String FLD_TENANT = "tenant";
  public static final String FLD_TYPE = "type";
  public static final String FLD_PRODUCT_ID = "productId";
  public static final String FLD_PRODUCT_TYPE_ID = "productTypeId";
  public static final String FLD_SOURCE_URL = "sourceUrl";
  public static final String PAGE_TYPE_TENANT_PRODUCT = "tenant_product_page";

}
