package com.beehyv.nutch.parse;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.storage.WebPage;

import java.nio.ByteBuffer;

/**
 * Created by haritha on 22/7/16.
 */
public class ProductParser extends BeehyvParser{

    protected String tenantName = "";
    protected String type = "";
    protected String productId = "";
    protected String productTypeId = "";


    public Parse getParse(String url, WebPage page) {
        Parse parse = super.getParse(url, page);

        page.getMetadata().put(ProductConstants.FLD_TYPE, ByteBuffer.wrap(type.getBytes()));
        page.getMetadata().put(ProductConstants.FLD_TENANT, ByteBuffer.wrap(tenantName.getBytes()));
        page.getMetadata().put(ProductConstants.FLD_PRODUCT_ID, ByteBuffer.wrap(productId.getBytes()));
        page.getMetadata().put(ProductConstants.FLD_PRODUCT_TYPE_ID, ByteBuffer.wrap(productTypeId.getBytes()));
        page.getMetadata().put(ProductConstants.FLD_SOURCE_URL, ByteBuffer.wrap(url.getBytes()));

        return parse;
    }

}
