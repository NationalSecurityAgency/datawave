package datawave.ingest.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.OptionsParser;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;

public class TableConfigCacheGenerator {
    protected static final Configuration config = new Configuration();

    protected static final Logger log = Logger.getLogger(TableConfigCache.class);

    public static void main(String[] args) {

        Configuration conf = OptionsParser.parseArguments(args, config);

        try {
            TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
            tcu.updateCacheFile();
        } catch (Exception e) {
            log.error("Unable to generate accumulo config cache " + e.getMessage());
        }
    }

}
