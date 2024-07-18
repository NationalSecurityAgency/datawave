package datawave.ingest.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.util.cli.PasswordConverter;

/**
 *
 * This class encapsulates the common options needed to connect to accumulo
 */
public class AccumuloCliOptions {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AccumuloCliOptions.class);
    private final Options options = new Options();
    private CommandLine cl = null;

    @SuppressWarnings("static-access")
    public AccumuloCliOptions() {

        options.addOption(Option.builder("u").argName("Username").hasArg().required().desc("Accumulo username").build());
        options.addOption(Option.builder("p").argName("Password").hasArg().required().desc("Accumulo password").build());
        options.addOption(Option.builder("i").argName("Instance Name").hasArg().required().desc("Accumulo instance name").build());
        options.addOption(Option.builder("zk").argName("ZooKeeper Servers").hasArg().required().desc("Comma separated list of ZooKeeper servers").build());
    }

    /**
     *
     * @param cl
     *            CommandLine containing accumulo Options
     * @return if commandline has the options
     */
    public boolean hasRequiredOptions(CommandLine cl) {
        return cl.hasOption("u") && cl.hasOption("p") && cl.hasOption("i") && cl.hasOption("zk");
    }

    /**
     *
     * @return accumulo options
     */
    public Options getOptions() {
        return options;
    }

    public void setCommandLine(CommandLine cl) {
        this.cl = cl;
    }

    /**
     *
     * @param args
     *            arguments to parse
     * @param loadDefaults
     *            load hadoop configuration defaults if set to true
     * @return configuration containing the properties needed to create an accumulo Connector
     */
    public Configuration getConf(String[] args, boolean loadDefaults) {
        Configuration conf = null;
        if (parseOptions(args)) {
            conf = new Configuration(loadDefaults);
            setAccumuloConfiguration(conf);
        }
        return conf;
    }

    public boolean parseOptions(String[] args) {
        try {
            cl = new DefaultParser().parse(options, args);
        } catch (ParseException ex) {
            log.error("Could not parse accumulo options", ex);
            return false;
        }
        return hasRequiredOptions(cl);
    }

    public void setAccumuloConfiguration(Configuration conf) {
        AccumuloHelper.setUsername(conf, cl.getOptionValue("u"));
        AccumuloHelper.setPassword(conf, PasswordConverter.parseArg(cl.getOptionValue("p")).getBytes());
        AccumuloHelper.setInstanceName(conf, cl.getOptionValue("i"));
        AccumuloHelper.setZooKeepers(conf, cl.getOptionValue("zk"));

    }

}
