package com.beehyv.nutch.parse;

import com.beehyv.munchbot.domainspecific.domcleaner.model.ExtractParams;
import com.beehyv.munchbot.domainspecific.enums.PageTypeEnum;
import com.beehyv.munchbot.ingestion.core.extractor.html.HtmlExtractor;
import com.beehyv.munchbot.ingestion.core.elasticsearch.IndexManager;
import com.beehyv.munchbot.ingestion.models.information.InfoNode;
import com.beehyv.munchbot.ingestion.models.information.InfoParagraph;
import com.beehyv.nutch.parse.rules.IngestionRuleModel;
import com.beehyv.nutch.parse.rules.PropertyValueParser;
import com.beehyv.nutch.parse.rules.RuleConstants;
import com.beehyv.nutch.parse.rules.RuleFilter;
import org.apache.commons.configuration.Configuration;
import org.apache.geronimo.mail.util.StringBufferOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.html.HtmlParser;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by haritha on 20/7/16.
 */
public class BeehyvParser extends HtmlParser {

    public static final String OUTLINKS_DISABLED = "nutch.parse.outlinks.disabled";
    public static final String DUMMY_USER = "nutch.parse.outlinks.dummyUser";
    public static final String USER_DUMMY_TRUE = "true";
    public static final String OUTLINKS_DISABLED_TRUE = "true";
    public static final String OUTLINKS_FILE = "nutch.parse.outlinks.file";
    public static final String PARSE_RULESFILE_DIR_LOC = "beehyv.nutch.parse.rulesfile.dir.loc";

    public static final Logger LOG = LoggerFactory
            .getLogger("com.beehyv.nutch.parse");

    public static final String USER_PARAM = "nutch.inject.beehyv.param";

    private RuleFilter ruleFilter = null;

    @Override
    public Parse getParse(String url, WebPage page) {
        System.out.println("In Beehyv parsing");
        Parse parse = super.getParse(url, page);
        String outlinksDisbled = super.getConf().get(OUTLINKS_DISABLED);
        String isUserDummy = super.getConf().get(DUMMY_USER);
        if(outlinksDisbled != null && outlinksDisbled.equals(OUTLINKS_DISABLED_TRUE)) {
            parse.setOutlinks(new Outlink[0]);
        }

        String outlinksFilePath = super.getConf().get(OUTLINKS_FILE);
        if(outlinksFilePath != null){
            PrintWriter writer = null;
            try{
               File outlinksFile = new File(outlinksFilePath);
               writer = new PrintWriter(outlinksFile);
               for(Outlink outlink : parse.getOutlinks())
                   writer.println(outlink.getToUrl());
           }
           catch (Exception e){

           }
           writer.close();
        }

        if(isUserDummy != null && isUserDummy.equals(USER_DUMMY_TRUE))
            return parse;

        String rulesDirLoc = super.getConf().get(PARSE_RULESFILE_DIR_LOC);
        ruleFilter = new RuleFilter(rulesDirLoc);
        try {

            Map<String, String> metadata = new HashMap<>();
            List<IngestionRuleModel> ingestionRuleModelList = ruleFilter.filterRules(url);
            PageTypeEnum pageType = PageTypeEnum.DEFAULT;
            List<String> whiteListCssSels = null;
            List<String> blackListCssSels = null;
            //TODO kapil - why is it assumed that 1st rule contains domain and tenant id
            metadata.put(RuleConstants.SOURCE_URL, url);
            if(ingestionRuleModelList.size() > 0) {
                int domainId = ingestionRuleModelList.get(0).getDomainId();
                int tenantId = ingestionRuleModelList.get(0).getTenantId();

                metadata.put(RuleConstants.DOMAIN_ID, String.valueOf(domainId));
                metadata.put(RuleConstants.TENANT_ID, String.valueOf(tenantId));
                page.getMetadata().put(RuleConstants.DOMAIN_ID, ByteBuffer.allocate(4).putInt(domainId));
                page.getMetadata().put(RuleConstants.TENANT_ID, ByteBuffer.allocate(4).putInt(tenantId));
            }

            
            for (IngestionRuleModel ruleModel : ingestionRuleModelList) {
                if (ruleModel.getProperty().equals(RuleConstants.PAGE_TYPE_ENUM)) {
                    pageType = PageTypeEnum.valueOf(
                            PropertyValueParser.getPropertyValue(url, ruleModel.getValue(), ruleModel.getValueType()));
                } else if(ruleModel.getProperty().equals(RuleConstants.WHITELIST_CSS_SELS)) {
                    String ruleCssSels = PropertyValueParser.getPropertyValue(url, ruleModel.getValue(), ruleModel.getValueType());
                    if(ruleCssSels != null && ruleCssSels.length()  > 0) {
                        whiteListCssSels = Arrays.asList(ruleCssSels.split(RuleConstants.RULESELS_SEP));
                    }
                } else if(ruleModel.getProperty().equals(RuleConstants.BLACKLIST_CSS_SELS)) {
                    String ruleCssSels =PropertyValueParser.getPropertyValue(url, ruleModel.getValue(), ruleModel.getValueType());
                    blackListCssSels = Arrays.asList(ruleCssSels.split(RuleConstants.RULESELS_SEP));
                }
                  else {
                    // properties example: productId and pagetype enum for ecomm
                    page.getMetadata().put(ruleModel.getProperty(),
                            ByteBuffer.wrap(PropertyValueParser.getPropertyValue(url, ruleModel.getValue(), ruleModel.getValueType()).getBytes()));
                }
                metadata.put(ruleModel.getProperty(),
                        PropertyValueParser.getPropertyValue(url, ruleModel.getValue(), ruleModel.getValueType()));
            }

            HtmlExtractor ext = new HtmlExtractor();
            ByteBuffer rawContent = page.getContent();
            ExtractParams extractParams = new ExtractParams(pageType, whiteListCssSels, blackListCssSels);

            String docCategory = getConf().get("doc.category");
            boolean isFAQ = docCategory != null && docCategory.equals("FAQ");
            LOG.debug("isFAQ document is sent as  " + isFAQ);
            InfoNode content = ext.extract(new ByteArrayInputStream(rawContent.array()),extractParams,url,isFAQ);
            if(content != null) {
                Map<String,String> map = content.getMetadata();
                if(map == null){
                    map = new HashMap<>();
                    content.setMetadata(map);
                }
                map.putAll(metadata);

                String fileName = url.substring(url.lastIndexOf("/")+1);
                fileName = fileName.replaceAll("[\\s]", "_");

                String uuid = UUID.randomUUID().toString();
                String reference = getConf().get(USER_PARAM)+"/"+fileName+"/"+uuid+"/json/";
//                String referenceContent = getConf().get(USER_PARAM)+fileName+"/"+uuid+"/content/";
//                String referenceText = getConf().get(USER_PARAM)+fileName+"/"+uuid+"/text/";

                ParserHelperUtil.setKeyInInfonode(content, reference);
                if(getConf().get("doc.category") != null){
                    ParserHelperUtil.setMetaDataInInfonode(content, "doc.category", getConf().get("doc.category"));
                }
                if(getConf().get("doc.websearch") != null){
                    ParserHelperUtil.setMetaDataInInfonode(content, "doc.websearch", getConf().get("doc.websearch"));
                }
                if(getConf().get("doc.websearch.provider") != null){
                    ParserHelperUtil.setMetaDataInInfonode(content, "doc.websearch.provider", getConf().get("doc.websearch.provider"));
                }

                ObjectMapper mapper = new ObjectMapper();
                String infoNodeString = mapper.writeValueAsString(content);
                startIndexing(infoNodeString);




                try{

                    InfonodeWriter.write(infoNodeString, reference, content);


                    page.getMetadata().put("json_status", ByteBuffer.wrap("Success writing JSON File".getBytes()));
//                    InfonodeWriter.write(page.getContent().toString(), referenceText);
//                    InfonodeWriter.write(page.getContent().toString(), referenceContent);
                }catch (IOException ex){
                    page.getMetadata().put("json_status", ByteBuffer.wrap("Error writing JSON File".getBytes()));
                }

                page.getMetadata().put("json", ByteBuffer.wrap(reference.getBytes()));
//                page.setText(referenceText);
//                page.setContent(ByteBuffer.wrap(referenceContent.getBytes()));

//                page.getMetadata().put("json", ByteBuffer.wrap(infoNodeString.getBytes()));
            }


            //page.setUserId(getConf().get(USER_PARAM));
        } catch (Exception e){
            LOG.error(e.getMessage(), e);
        }
        return parse;
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
