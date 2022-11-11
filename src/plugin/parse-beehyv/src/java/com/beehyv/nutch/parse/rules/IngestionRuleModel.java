package com.beehyv.nutch.parse.rules;

import java.util.List;

/**
 * Created by kapil on 25/10/16.
 */
public class IngestionRuleModel {

    private Integer domainId;
    private Integer tenantId;

    // this is the main field, based on which other properties would be assigned
    private String urlRegex;
    // e.g. productId, page type for this url pattern
    private String property;
    // how this property should be fetched
    private String value;
    // type of the value could be xPath or some other format
    private String valueType;
    private List<String> payloadKeys;

    public Integer getDomainId() {
        return domainId;
    }

    public void setDomainId(Integer domainId) {
        this.domainId = domainId;
    }

    public Integer getTenantId() {
        return tenantId;
    }

    public void setTenantId(Integer tenantId) {
        this.tenantId = tenantId;
    }

    public String getUrlRegex() {
        return urlRegex;
    }

    public void setUrlRegex(String urlRegex) {
        this.urlRegex = urlRegex;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public List<String> getPayloadKeys() {
        return payloadKeys;
    }

    public void setPayloadKeys(List<String> payloadKeys) {
        this.payloadKeys = payloadKeys;
    }
}
