package datawave.ingest.csv.mr.handler;

import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ContentCSVColumnBasedHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {
    
    private Map<String,Set<String>> subtypeFieldTokenizationWhitelistMap = new HashMap<String,Set<String>>();
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
        ExtendedCSVIngestHelper myHelper = (ExtendedCSVIngestHelper) helper;
        this.subtypeFieldTokenizationWhitelistMap = myHelper.getSubtypeFieldTokenizationWhitelistMap();
    }
    
    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (ExtendedCSVIngestHelper) helper;
    }
    
    @Override
    protected boolean isTokenizationBySubtypeEnabled() {
        return true;
    }
    
    @Override
    protected boolean determineTokenizationBySubtype(String field) {
        
        if (this.subtypeFieldTokenizationWhitelistMap.containsKey(this.eventDataTypeName)) {
            return this.subtypeFieldTokenizationWhitelistMap.get(this.eventDataTypeName).contains(field.toUpperCase().trim());
        }
        
        return true;
    }
}
