package datawave.ingest.util;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.shard.NumShards;

public class GenerateMultipleNumShardsCacheFile {
    private static final Logger log = Logger.getLogger(GenerateMultipleNumShardsCacheFile.class);

    public static final String MULTIPLE_NUMSHARD_CACHE_FILE_LOCATION_OVERRIDE = "ns";
    public static final String CONFIG_DIRECTORY_LOCATION_OVERRIDE = "cd";
    public static final String CONFIG_SUFFIEX_OVERRIDE = "cs";

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws ParseException, AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
        AccumuloCliOptions accumuloOptions = new AccumuloCliOptions();
        Options options = accumuloOptions.getOptions();
        options.addOption(OptionBuilder.isRequired(true).hasArg().withDescription("Config directory path").create(CONFIG_DIRECTORY_LOCATION_OVERRIDE));
        options.addOption(OptionBuilder.isRequired(false).hasArg().withDescription("Config file suffix").create(CONFIG_SUFFIEX_OVERRIDE));
        options.addOption(OptionBuilder.isRequired(false).hasArg().withDescription("Multiple numShards cache file path")
                        .create(MULTIPLE_NUMSHARD_CACHE_FILE_LOCATION_OVERRIDE));
        Configuration conf = accumuloOptions.getConf(args, true);
        CommandLine cl;
        String configDirectory = null;
        String configSuffix;
        try {
            cl = new DefaultParser().parse(options, args);
            if (cl.hasOption(CONFIG_DIRECTORY_LOCATION_OVERRIDE)) {
                configDirectory = cl.getOptionValue(CONFIG_DIRECTORY_LOCATION_OVERRIDE);
            } else {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("Generate Multiple NumShards Cache", options);
                System.exit(1);
            }
            if (cl.hasOption(MULTIPLE_NUMSHARD_CACHE_FILE_LOCATION_OVERRIDE)) {
                conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, cl.getOptionValue(MULTIPLE_NUMSHARD_CACHE_FILE_LOCATION_OVERRIDE));
            }
            if (cl.hasOption(CONFIG_SUFFIEX_OVERRIDE)) {
                configSuffix = cl.getOptionValue(CONFIG_SUFFIEX_OVERRIDE);
            } else {
                configSuffix = "config.xml";
            }
            ConfigurationFileHelper.setConfigurationFromFiles(conf, configDirectory, configSuffix);
            NumShards numShards = new NumShards(conf);
            numShards.updateCache();
        } catch (ParseException ex) {
            log.error(GenerateMultipleNumShardsCacheFile.class.getName(), ex);
        }
    }
}
