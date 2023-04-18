package datawave.ingest;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class TableCreator {
    
    private static Configuration config = new Configuration();
    
    private static Logger log = Logger.getLogger(TableCreator.class);
    
    public static void main(String[] args) {
        Configuration conf = OptionsParser.parseArguments(args, config);
        try {
            TableConfigurationUtil tableConfigUtil = new TableConfigurationUtil(conf);
            tableConfigUtil.registerTableNamesFromConfigFiles(conf);
            tableConfigUtil.configureTables(conf);
        } catch (Exception e) {
            log.error("Unable to create tables", e);
        }
    }
    
}
