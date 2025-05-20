package datawave.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;

public class TableCreator {

    private static final Configuration config = new Configuration();

    private static final Logger log = Logger.getLogger(TableCreator.class);

    public static void main(String[] args) {
        Configuration conf = OptionsParser.parseArguments(args, config);
        try {
            TableConfigurationUtil tableConfigUtil = new TableConfigurationUtil(conf);
            TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
            tableConfigUtil.configureTables(conf);
        } catch (Exception e) {
            log.error("Unable to create tables", e);
        }
    }

}
