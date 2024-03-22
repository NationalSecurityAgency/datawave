package datawave.ingest.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import datawave.ingest.mapreduce.job.TableSplitsCache;

/**
 *
 * This utility will update splits cache file
 */
public class GenerateSplitsFile {

    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(GenerateSplitsFile.class);

    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        AccumuloCliOptions accumuloOptions = new AccumuloCliOptions();
        Options options = accumuloOptions.getOptions();
        options.addOption(OptionBuilder.isRequired(true).hasArg().withDescription("Config directory path").create("cd"));
        options.addOption(OptionBuilder.isRequired(false).hasArg().withDescription("Config file suffix").create("cs"));
        options.addOption(OptionBuilder.isRequired(false).hasArg().withDescription("Splits file path").create("sp"));
        Configuration conf = accumuloOptions.getConf(args, true);
        CommandLine cl;
        String configDirectory = null;
        String configSuffix;
        try {
            cl = new DefaultParser().parse(options, args);
            if (cl.hasOption("cd")) {
                configDirectory = cl.getOptionValue("cd");
                log.info("Set configDirectory to " + configDirectory);
            } else {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("Generate Splits", options);
                System.exit(1);
            }
            if (cl.hasOption("sp")) {
                conf.set(TableSplitsCache.SPLITS_CACHE_DIR, cl.getOptionValue("sp"));
                log.info("Set " + TableSplitsCache.SPLITS_CACHE_DIR + " to " + cl.getOptionValue("sp"));
            }
            if (cl.hasOption("cs")) {
                configSuffix = cl.getOptionValue("cs");
            } else {
                configSuffix = "config.xml";
            }
            log.info("Set configSuffix to " + configSuffix);

            ConfigurationFileHelper.setConfigurationFromFiles(conf, configDirectory, configSuffix);
            TableSplitsCache splitsFile = TableSplitsCache.getCurrentCache(conf);
            splitsFile.update();
        } catch (ParseException ex) {
            log.error(GenerateSplitsFile.class.getName(), ex);
        }
    }
}
