package com.beehyv.nutch.parse.rules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kapil on 26/10/16.
 */
public class CsvRuleBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvRuleBuilder.class);
    private static final String RULES_FILE = "rules.csv";

    private String rulesDirLoc;
    public CsvRuleBuilder(String rulesLoc) {
        this.rulesDirLoc = rulesLoc;
    }


    public List<IngestionRuleModel> buildRules() {
        List<IngestionRuleModel> allRulesList = new ArrayList<>();
        ICsvBeanReader beanReader = null;
        try {
            if(rulesDirLoc == null) {
                return allRulesList;
            }
            InputStream inputStream = new FileInputStream(rulesDirLoc + RULES_FILE);
            if(inputStream != null) {
                Reader reader = new InputStreamReader(inputStream);
                beanReader = new CsvBeanReader(reader, CsvPreference.STANDARD_PREFERENCE);

                // the header elements are used to map the values to the bean (names must match)
                final String[] header = beanReader.getHeader(true);
                final CellProcessor[] processors = getProcessors();
                IngestionRuleModel ruleModel;
                while( (ruleModel = beanReader.read(IngestionRuleModel.class, header,processors)) != null ) {
                    allRulesList.add(ruleModel);
                }
            }
        }
        catch(Exception e) {
            LOGGER.error(e.getMessage(),e);
        }
        finally {
            try {
                if( beanReader != null ) {
                    beanReader.close();
                }
            }
            catch(IOException e) {
                LOGGER.error(e.getMessage(),e);
            }
        }
        return allRulesList;
    }

    // domainId, tenantId, urlRegex, property, value, valueType, payloadKeys
    private static CellProcessor[] getProcessors() {

        final CellProcessor[] processors = new CellProcessor[] {
                new ParseInt(),
                new ParseInt(),
                new Optional(),
                new Optional(),
                new Optional(),
                new Optional(),
                new Optional()
        };

        return processors;
    }




}
