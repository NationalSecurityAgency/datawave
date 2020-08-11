package datawave.ingest.config;

import datawave.ingest.OptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class TableConfigCacheGenerator {
    protected static Configuration config = new Configuration();
    
    protected static Logger log = Logger.getLogger(TableConfigCache.class);
    
    public static void main(String[] args) {
        
        Configuration conf = OptionsParser.parseArguments(args, config);
        TableConfigCache cache = new TableConfigCache(conf);
        
        try {
            cache.update();
        } catch (Exception e) {
            log.error("Unable to generate accumulo config cache " + e.getMessage());
        }
    }
    
}
