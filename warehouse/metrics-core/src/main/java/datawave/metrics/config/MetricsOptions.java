package datawave.metrics.config;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * An extension of Apache CLI Options that specify runtime arguments to the MetricsIngester. Setting these values at runtime will override values in the
 * metrics.xml configuration file.
 *
 */
public class MetricsOptions extends Options {

    private static final long serialVersionUID = 133461729293349960L;

    @SuppressWarnings("static-access")
    public MetricsOptions() {
        super();

        this.addOption(Option.builder("zookeepers").argName("servers").hasArg().desc("Comma separated list of ZooKeeper servers").build());
        this.addOption(Option.builder("user").argName("user").hasArg().desc("Accumulo user name").build());
        this.addOption(Option.builder("password").argName("password").hasArg().desc("Accumulo user password").build());
        this.addOption(Option.builder("defaultvis").argName("defaultvis").hasArg().desc("Default visibility for certain Accumulo keys").build());
        this.addOption(Option.builder("instance").argName("instance").hasArg().desc("Accumulo user password").build());
        this.addOption(Option.builder("type").argName("\"ingest\"|\"loader\"").hasArg().desc("Type of metrics being ingested").build());
        this.addOption(Option.builder("input").argName("HDFS directory").hasArg().desc("Source of input files to MR job").build());
        this.addOption(Option.builder("conf").argName("Path to metrics.xml").hasArg().desc("Path to the metrics.xml configuration file.").build());
        this.addOption(Option.builder("hdfsConf").argName("Path to hdfs-site.xml").hasArg().desc("Path to the HDFS Configuration file").build());
        this.addOption(Option.builder("war").argName("Path to webapp war file.").hasArg().desc("Path to the metrics web app war file.").build());
        this.addOption(Option.builder("start").argName("Start date to begin processing metrics.").hasArg()
                        .desc("Start date to begin processing metrics, as yyyyMMddHHmm or yyyyMMdd.").build());
        this.addOption(Option.builder("end").argName("Stop date for processing metrics.").hasArg()
                        .desc("Stop date for processing metrics, as yyyyMMddHHmm or yyyyMMdd.").build());
        this.addOption(Option.builder("ignoreFlagMakerMetrics").argName("Ignore flag maker metrics for correlation").hasArg()
                        .desc("Ignore flag maker metrics for correlation").build());
    }

}
