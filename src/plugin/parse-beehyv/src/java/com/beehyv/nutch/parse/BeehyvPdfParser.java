package com.beehyv.nutch.parse;

import com.beehyv.munchbot.ingestion.core.extractor.*;
import com.beehyv.munchbot.ingestion.core.extractor.pdf.*;
import com.beehyv.munchbot.ingestion.core.extractor.pdf.pdfbox.*;
import com.beehyv.munchbot.ingestion.models.*;
import com.beehyv.munchbot.ingestion.models.pdf.HolmesPdfDocument;
import com.beehyv.munchbot.ingestion.models.information.*;

import java.io.*;

import org.apache.avro.util.Utf8;
import org.apache.fontbox.util.Charsets;
import org.apache.geronimo.mail.util.StringBufferOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.util.TextUtils;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.*;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

/**
 * Created by kapil on 23/9/16.
 */
public class BeehyvPdfParser implements Parser {

    public static final Logger LOG = LoggerFactory.getLogger(BeehyvPdfParser.class);
    public static final String USER_PARAM = "nutch.inject.beehyv.param";

    private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

    static {
        FIELDS.add(WebPage.Field.BASE_URL);
        FIELDS.add(WebPage.Field.CONTENT_TYPE);
    }

    private Configuration conf;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Parse getParse(String url, WebPage page) { //throws Exception {
        LOG.debug("Parsing %s", url);
        Parse parse = null;

        String pdfText, title ="";
        PdfExtractor extractor = null;// PdfExtractor.getInstance(url);
        try {
            extractor = new PdfExtractor(new PdfBoxExtractor());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }
	try {
        String mimeType = page.getContentType().toString();
        LOG.debug("mimetype is: " + mimeType);
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, mimeType);
        String docCategory = getConf().get("doc.category");

        InfoNode pdfDoc = extractor.extractUrl(url,docCategory != null && docCategory.equals("FAQ"));

        String fileName = url.substring(url.lastIndexOf("/")+1, url.lastIndexOf(".pdf"));
        fileName = fileName.replaceAll("[\\s]", "_");
        String uuid = UUID.randomUUID().toString();
        String reference = getConf().get(USER_PARAM)+"/"+fileName+"/"+uuid+"/json/";
//        String referenceContent = getConf().get(USER_PARAM)+fileName+"/"+uuid+"/content/";
//        String referenceText = getConf().get(USER_PARAM)+fileName+"/"+uuid+"/text/";

        ParserHelperUtil.setKeyInInfonode(pdfDoc, reference);
        if(getConf().get("doc.category") != null){
            ParserHelperUtil.setMetaDataInInfonode(pdfDoc, "doc.category", getConf().get("doc.category"));
        }
        if(getConf().get("doc.websearch") != null){
            ParserHelperUtil.setMetaDataInInfonode(pdfDoc, "doc.websearch", getConf().get("doc.websearch"));
        }
        if(getConf().get("doc.websearch.provider") != null){
            ParserHelperUtil.setMetaDataInInfonode(pdfDoc, "doc.websearch.provider", getConf().get("doc.websearch.provider"));
        }

        String infoNodeString = mapper.writeValueAsString(pdfDoc);
        LOG.debug("InfoNode String %s", infoNodeString);

        if(pdfDoc == null)
            LOG.debug("InfoNode null");
        else {
            startIndexing(infoNodeString);
        }


        pdfText = pdfDoc.getContent();
        title = pdfDoc.getMetadata().get("title");
        if(title == null) {
            LOG.debug("Title is null, hence setting empty string- else ParseUtil gives error");
            title = "";
        }
        LOG.debug("Title %s", title);
        Outlink[] outlinks = OutlinkExtractor.getOutlinks(pdfText, getConf());
//        metadata.add(Metadata.DESCRIPTION, "No of pages: " + String.valueOf(pdfDoc.getNoOfPages()));
        metadata.add(Metadata.TITLE, title);

        ParseStatus status = ParseStatusUtils.STATUS_SUCCESS;
        parse = new Parse(pdfText, title, outlinks, status);
        // populate Nutch metadata with our gathered metadata
        String[] pdfMDNames = metadata.names();
        for (String pdfMDName : pdfMDNames) {
            if (!TextUtils.isEmpty(metadata.get(pdfMDName)))
                page.getMetadata().put(new Utf8(pdfMDName),
                        ByteBuffer.wrap(Bytes.toBytes(metadata.get(pdfMDName))));
        }

       // try{
            InfonodeWriter.write(infoNodeString, reference, pdfDoc);
            page.getMetadata().put("json_status", ByteBuffer.wrap("Success writing JSON File".getBytes()));
	    page.getMetadata().put("json", ByteBuffer.wrap(infoNodeString.getBytes()));
//            InfonodeWriter.write(page.getContent().toString(), referenceContent);
//            InfonodeWriter.write(page.getContent().toString(), referenceText);
        }catch (Exception ex){
	    LOG.error(ex.getMessage(), ex);
            page.getMetadata().put("json_status", ByteBuffer.wrap("Error writing JSON File".getBytes()));
        }
       // page.getMetadata().put("json", ByteBuffer.wrap(reference.getBytes()));
//        page.setContent(ByteBuffer.wrap(referenceContent.getBytes()));
//        page.setText(referenceText);

//        page.getMetadata().put("json", ByteBuffer.wrap(infoNodeString.getBytes()));
        //page.setUserId(getConf().get(USER_PARAM));
        return parse;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public Collection<WebPage.Field> getFields() {
        return FIELDS;
    }

    private void startIndexing(String infoNodeString) throws Exception {
        String batchId = "batchId" + System.currentTimeMillis();
        org.apache.hadoop.conf.Configuration config = NutchConfiguration.create();
        config.set("elastic.host","localhost");
        config.set("elastic.port","9300");
        config.set("elastic.index",getConf().get(USER_PARAM));
        config.set("elastic.infonode",infoNodeString);
        int res = ToolRunner.run(config,new IndexingJob(),new String[]{batchId});
    }

}
