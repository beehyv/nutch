package com.beehyv.nutch.indexer;


import org.apache.avro.util.Utf8;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;

/**
 * Created by haritha on 22/7/16.
 */
public class ProductIndexingFilter extends BeehyvIndexingFilter {
    @Override
    public NutchDocument filter(NutchDocument doc, String url,
                                WebPage page) throws IndexingException {
        NutchDocument outputDoc = super.filter(doc,url,page);
        outputDoc.add(Constants.FLD_PRODUCT_ID, new String(page.getMetadata().get(new Utf8(Constants.FLD_PRODUCT_ID)).array()));
        outputDoc.add(Constants.FLD_PRODUCT_TYPE_ID, new String(page.getMetadata().get(new Utf8(Constants.FLD_PRODUCT_TYPE_ID)).array()));
        outputDoc.add(Constants.FLD_SOURCE_URL, new String(page.getMetadata().get(new Utf8(Constants.FLD_SOURCE_URL)).array()));
        outputDoc.add(Constants.FLD_TENANT, new String(page.getMetadata().get(new Utf8(Constants.FLD_TENANT)).array()));
        outputDoc.add(Constants.FLD_TYPE, new String(page.getMetadata().get(new Utf8(Constants.FLD_TYPE)).array()));

        return outputDoc;
    }
}
