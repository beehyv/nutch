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

import com.beehyv.munchbot.ingestion.core.elasticsearch.ElasticSearchClient;
import com.beehyv.munchbot.ingestion.core.elasticsearch.IndexManager;
import com.beehyv.munchbot.ingestion.models.information.InfoNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class BeehyvIndexWriter implements IndexWriter {
    public static Logger LOG = LoggerFactory.getLogger(BeehyvIndexWriter.class);

    private static final int DEFAULT_MAX_BULK_DOCS = 250;
    private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;
    private  Client client;
    private IndexManager indexManager;
    private Node node;
    private String defaultIndex;
    private InfoNode infoNode;
    private String infoNodeString;

    private Configuration config;

    private BulkRequestBuilder bulk;
    private ListenableActionFuture<BulkResponse> execute;
    private int port = -1;
    private String host = null;
    private String clusterName = null;
    private int maxBulkDocs;
    private int maxBulkLength;
    private long indexedDocs = 0;
    private int bulkDocs = 0;
    private int bulkLength = 0;
    private boolean createNewBulk = false;
    private ObjectMapper mapper = new ObjectMapper();
    private ElasticSearchClient esClient;

//    static {
//        try {
//            if (esClient == null || client == null) {
//                esClient = new ElasticSearchClient();
//                client = esClient.getClient();
//                LOG.info("Created new ElasticSearchClient: ");
//            }
//        } catch (Exception e) {
//            LOG.error("Could not init ElasticSearchClient: " + e);
//        }
//    }

    
    @Override
    public void open(Configuration job) throws IOException {
        LOG.info("BeehyvIndexWriter: open");
        clusterName = job.get(BeehyvElasticConstants.CLUSTER);
        host = job.get(BeehyvElasticConstants.HOST);
        port = job.getInt(BeehyvElasticConstants.PORT, 9300);
        infoNodeString = job.get("elastic.infonode");

        LOG.info("**************************");
        LOG.info("host:port:cluster " + host + ":" + port + ":" + clusterName);

        /*
        Builder settingsBuilder = Settings.settingsBuilder();

        if (StringUtils.isNotBlank(clusterName))
            settingsBuilder.put("cluster.name", clusterName);

        // Set the cluster name and build the settings
        Settings settings = settingsBuilder.build(); */
        esClient = ElasticSearchClient.getInstance();
        client = esClient.getClient();
        System.out.println(client);
        indexManager = new IndexManager(esClient);

        LOG.info("**************************");


        bulk = client.prepareBulk();
        defaultIndex = job.get(BeehyvElasticConstants.INDEX, "elastic.index");
        LOG.info("defaultIndex" + defaultIndex);
        maxBulkDocs = job.getInt(BeehyvElasticConstants.MAX_BULK_DOCS,
                DEFAULT_MAX_BULK_DOCS);
        maxBulkLength = job.getInt(BeehyvElasticConstants.MAX_BULK_LENGTH,
                DEFAULT_MAX_BULK_LENGTH);
        LOG.info("Checking for Index");
        if (!indexManager.checkIfIndexExists(defaultIndex)) {
            indexManager.createIndex(defaultIndex);
            LOG.info("Created the Index with Mappings");
        } else {
            LOG.info("already created");
        }
        infoNode = mapper.readValue(infoNodeString, InfoNode.class);
    }

    @Override
    public void write(NutchDocument doc) throws IOException {

    }

    @Override
    public void delete(String key) throws IOException {
        try {
            DeleteRequestBuilder builder = client.prepareDelete();
            builder.setIndex(defaultIndex);
            builder.setType("doc");
            builder.setId(key);
            builder.execute().actionGet();
        } catch (ElasticsearchException e) {
            throw makeIOException(e);
        }
    }

    public static IOException makeIOException(ElasticsearchException e) {
        final IOException ioe = new IOException();
        ioe.initCause(e);
        return ioe;
    }

    @Override
    public void update(NutchDocument doc) throws IOException {
        write(doc);
    }

    @Override
    public void commit() throws IOException {
        indexManager.indexInfoNode(infoNode, defaultIndex);
    }

    @Override
    public void close() throws IOException {
        commit();
//        client.close();
        if (node != null) {
            node.close();
        }
    }

    @Override
    public String describe() {
        StringBuffer sb = new StringBuffer("BeehyvIndexWriter\n");
        sb.append("\t").append(BeehyvElasticConstants.CLUSTER)
                .append(" : elastic prefix cluster\n");
        sb.append("\t").append(BeehyvElasticConstants.HOST).append(" : hostname\n");
        sb.append("\t").append(BeehyvElasticConstants.PORT)
                .append(" : port  (default 9300)\n");
        sb.append("\t").append(BeehyvElasticConstants.INDEX)
                .append(" : elastic index command \n");
        sb.append("\t").append(BeehyvElasticConstants.MAX_BULK_DOCS)
                .append(" : elastic bulk index doc counts. (default 250) \n");
        sb.append("\t").append(BeehyvElasticConstants.MAX_BULK_LENGTH)
                .append(" : elastic bulk index length. (default 2500500 ~2.5MB)\n");
        return sb.toString();
    }

    @Override
    public void setConf(Configuration conf) {
        config = conf;
        String cluster = conf.get(BeehyvElasticConstants.CLUSTER);
        String host = conf.get(BeehyvElasticConstants.HOST);

        if (StringUtils.isBlank(cluster) && StringUtils.isBlank(host)) {
            String message = "Missing elastic.cluster and elastic.host. At least one of them should be set in nutch-site.xml ";
            message += "\n" + describe();
            LOG.error(message);
            throw new RuntimeException(message);
        }
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }
}
