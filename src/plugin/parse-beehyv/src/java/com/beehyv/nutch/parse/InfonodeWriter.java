package com.beehyv.nutch.parse;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nutch.util.NutchConfiguration;
import com.beehyv.munchbot.ingestion.core.elasticsearch.IndexManager;
import com.beehyv.munchbot.ingestion.core.elasticsearch.ElasticSearchClient;
import org.codehaus.jackson.map.ObjectMapper;
import com.beehyv.munchbot.ingestion.models.information.InfoNode;

public class InfonodeWriter {
    static Configuration conf = NutchConfiguration.create();

    static {
//        conf.addResource(new Path("file:///home/beehyv/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));
//        conf.set("hadoop.security.authentication", "kerberos");
//        conf.set("fs.defaultFS", "hdfs://node-master:9000");
//        UserGroupInformation.setConfiguration(conf);
    }

    public static void write(String infoNodeString, String reference, InfoNode node) throws IOException {
        Path path = new Path(reference);
        FileSystem hdfs = path.getFileSystem(conf);
        System.out.println(hdfs.getWorkingDirectory());
        hdfs.mkdirs(path.getParent());
        OutputStream os = hdfs.create(path);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write(infoNodeString);
        br.close();
        hdfs.close();
        //Since index is not working after upgrading to nutch latest in 2022,
        //this is just added as hack for demo
        //the only issue with this will be if we really leverage 100's of hadoop noes
        //also username is put as "admin" temporariy !
        IndexManager manager = new IndexManager(ElasticSearchClient.getInstance());
//        ObjectMapper objectMapper = new ObjectMapper();
//        InfoNode node = objectMapper.readValue(infoNodeString, InfoNode.class);
        manager.indexInfoNode(node , "admin");
    }
}
