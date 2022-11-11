package com.beehyv.nutch.parse;

import com.beehyv.munchbot.ingestion.core.extractor.AbstractExtractor;
import com.beehyv.munchbot.ingestion.core.extractor.doc.DocExtractor;
import com.beehyv.munchbot.ingestion.core.extractor.doc.DocxExtractor;
import com.beehyv.munchbot.ingestion.models.information.InfoNode;
import com.beehyv.munchbot.ingestion.models.information.InfoParagraph;
import org.apache.geronimo.mail.util.StringBufferOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.*;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by kapil on 26/9/16.
 */
public class BeehyvDocxParser implements Parser {

    public static final String USER_PARAM = "nutch.inject.beehyv.param";

    public static final Logger LOG = LoggerFactory.getLogger(BeehyvDocxParser.class);

    private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

    static {
        FIELDS.add(WebPage.Field.BASE_URL);
        FIELDS.add(WebPage.Field.CONTENT_TYPE);
    }

    private Configuration conf;
    private final String DOC_TYPE = "application/msword";
    private final String DOCX_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

    @Override
    public Parse getParse(String url, WebPage page) { //throws Exception {
        String mimeType = page.getContentType().toString();
        LOG.debug("mimetype is: " + mimeType);

        InfoNode documentContent = null;
        AbstractExtractor extractor = null;

        switch (mimeType) {
            case DOC_TYPE:
                // uses docExtractor
                extractor = new DocExtractor();
                break;
            case DOCX_TYPE:
                // uses docxExtractor
                extractor = new DocxExtractor();
                break;
            default:
                extractor = new DocxExtractor();
                break;
        }
        try{
        documentContent = extractor.extractUrl(url);


        String fileName = url.substring(url.lastIndexOf("/")+1, url.lastIndexOf(".doc"));
        fileName = fileName.replaceAll("[\\s]", "_");

        String uuid = UUID.randomUUID().toString();
        String reference = getConf().get(USER_PARAM)+"/"+fileName+"/"+uuid+"/json/";
//        String referenceContent = getConf().get(USER_PARAM)+fileName+"/"+uuid+"/content/";
//        String referenceText = getConf().get(USER_PARAM)+fileName+"/"+uuid+"/text/";

        ParserHelperUtil.setKeyInInfonode(documentContent, reference);
        if(getConf().get("doc.category") != null){
            ParserHelperUtil.setMetaDataInInfonode(documentContent, "doc.category", getConf().get("doc.category"));
        }
        if (getConf().get("doc.websearch") != null) {
            ParserHelperUtil.setMetaDataInInfonode(documentContent, "doc.websearch", getConf().get("doc.websearch"));
        }
        if (getConf().get("doc.websearch.provider") != null) {
            ParserHelperUtil.setMetaDataInInfonode(documentContent, "doc.websearch.provider", getConf().get("doc.websearchprovider"));
        }

        ObjectMapper mapper = new ObjectMapper();

        String infoNodeString = mapper.writeValueAsString(documentContent);
        startIndexing(infoNodeString);

            InfonodeWriter.write(infoNodeString, reference,documentContent);
            page.getMetadata().put("json_status", ByteBuffer.wrap("Success writing JSON File".getBytes()));
	    page.getMetadata().put("json", ByteBuffer.wrap(reference.getBytes()));
//            InfonodeWriter.write(page.getContent().toString(), referenceContent);
//            InfonodeWriter.write(page.getContent().toString(), referenceText);
        }catch (Exception ex){
            	LOG.error(ex.getMessage() , ex);
		page.getMetadata().put("json_status", ByteBuffer.wrap("Error writing JSON File".getBytes()));
        }

        //page.getMetadata().put("json", ByteBuffer.wrap(reference.getBytes()));
//        page.setContent(ByteBuffer.wrap(referenceContent.getBytes()));
//        page.setText(referenceText);

//        page.getMetadata().put("json", ByteBuffer.wrap(infoNodeString.getBytes()));
        // we do not want any outlinks
        Outlink[] outlinks = new Outlink[0];
//        if (!TextUtils.isEmpty(documentContent.getRawContent()))
//            outlinks = OutlinkExtractor.getOutlinks(documentContent.getRawContent(), getConf());
//        else outlinks = new Outlink[0];
        ParseStatus status = ParseStatusUtils.STATUS_SUCCESS;
        String docContent ="";
        String title = "";
        if(documentContent != null) {
            docContent = documentContent.getContent();
            Map<String,String> mtdtMap = documentContent.getMetadata();
            if(mtdtMap != null) {
                mtdtMap.get("title");
            }
        }


        //page.setUserId(getConf().get(USER_PARAM));
        return new Parse(docContent, title, outlinks, status);
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
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
