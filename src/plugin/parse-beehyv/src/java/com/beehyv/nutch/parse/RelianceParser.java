package com.beehyv.nutch.parse;

import com.beehyv.munchbot.domainspecific.enums.ETenant;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.storage.WebPage;

/**
 * Created by haritha on 22/7/16.
 */
public class RelianceParser extends ProductParser {

    private ETenant tenant = ETenant.RELIANCE;
    public RelianceParser() {
        super();
        tenantName = tenant.getName();
//        productId = "snapdealKaptaan";
    }


    public Parse getParse(String url, WebPage page) {
//        try {
////            productId = Utils.getProductIdFromUrl(url, tenant);
//        } catch(ArrayIndexOutOfBoundsException e){
//            LOG.error("The wrong parser seems to be called !!!! ");
//        }
        type = ProductConstants.PAGE_TYPE_TENANT_PRODUCT;
        return super.getParse(url, page);
    }
}
