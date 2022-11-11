package com.beehyv.nutch.parse;

import com.beehyv.munchbot.ingestion.models.information.InfoNode;
import com.beehyv.munchbot.ingestion.models.information.InfoParagraph;

import java.util.HashMap;

public class ParserHelperUtil {

    public static void setKeyInInfonode(InfoNode infoNode, String key) {
        infoNode.setKey(key);
        if (infoNode.getParagraphs() != null) {
            for(InfoParagraph para : infoNode.getParagraphs()) {
                para.setKey(key);
            }
        }

        if (infoNode.getSections() != null) {
             for(InfoNode node : infoNode.getSections()){
                setKeyInInfonode(node, key);
            }
        }

    }


    /**
     * Saves metadata in ALL nodes
     * @param infoNode
     * @param key
     * @param value
     */
    public static void setMetaDataInInfonode(InfoNode infoNode, String key, String value){
        if(infoNode.getMetadata() == null)
            infoNode.setMetadata(new HashMap<String, String>());
        infoNode.getMetadata().put(key, value);
        if(infoNode.getParagraphs()!=null) {
            for (InfoParagraph p : infoNode.getParagraphs()) {
                if (p.getMetadata() == null)
                    p.setMetadata(new HashMap<String, String>());
                p.getMetadata().put(key, value);
            }
        }
        if(infoNode.getSections()!=null) {
            for (InfoNode node : infoNode.getSections()) {
                setMetaDataInInfonode(node, key, value);
            }
        }
    }
}
