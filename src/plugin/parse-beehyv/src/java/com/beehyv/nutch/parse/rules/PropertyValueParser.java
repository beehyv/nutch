package com.beehyv.nutch.parse.rules;


import com.beehyv.munchbot.ingestion.core.extractor.html.HtmlExtractor;

/**
 * Created by kapil on 1/11/16.
 *
 * Gets the value of a property based on the value type
 */
public class PropertyValueParser {

    /**
     * Returns string repr of value of a property
     * @param url url of this page
     * @param value how this value should be calculated
     * @param valueType type of this value
     * @return value
     */
    public static String getPropertyValue(String url, String value, String valueType) {
        String returnValue = "";
        switch (valueType) {
            case "selectorPath":
                // need Jsoup document here
                /** Commenting since after upgrading to nutch 2.4 jsoup is giving error, though using same version
                 * also this is never used -- this and xPath are for getting specific properties from rules
                 * java.lang.LinkageError: loader constraint violation: when resolving method "org.jsoup.nodes.Document.child(I)Lorg/jsoup/nodes/Element;" the class loader (instance of org/apache/nutch/plugin/PluginClassLoader) of the current class, , and the class loader (instance of org/apache/catalina/loader/ParallelWebappClassLoader) for the method's defining class, org/jsoup/nodes/Element, have different Class objects for the typeused in the signature
                 */
               // Document document = new HtmlExtractor().extractDoc(url);
                //returnValue = document.select(value).attr("value");
                break;
            case "xPath":
                // convert to Jsoup query and execute
                break;
            case "text":
                // value is the returnValue
                returnValue = value;
                break;
        }
        return returnValue;

    }
}
