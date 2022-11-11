package com.beehyv.nutch.indexer;

import org.apache.avro.util.Utf8;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.basic.BasicIndexingFilter;
import org.apache.nutch.storage.WebPage;

/**
 * Created by haritha on 20/7/16.
 */
public class BeehyvIndexingFilter extends BasicIndexingFilter {
    @Override
    public NutchDocument filter(NutchDocument doc, String url,
                                WebPage page) throws IndexingException {
        NutchDocument outputDoc = super.filter(doc,url,page);
        outputDoc.add(Constants.FLD_JSON, new String(page.getMetadata().get(new Utf8("json")).array()));
        return outputDoc;
    }
}
