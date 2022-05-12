package datawave.metrics.config;

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
        super.addOption(OptionBuilder.withArgName("servers").hasArg().withDescription("Comma separated list of ZooKeeper servers").create("zookeepers"));
        
        super.addOption(OptionBuilder.withArgName("user").hasArg().withDescription("Accumulo user name").create("user"));
        
        super.addOption(OptionBuilder.withArgName("password").hasArg().withDescription("Accumulo user password").create("password"));
        
        super.addOption(OptionBuilder.withArgName("defaultvis").hasArg().withDescription("Default visibility for certain Accumulo keys").create("defaultvis"));
        
        super.addOption(OptionBuilder.withArgName("instance").hasArg().withDescription("Accumulo user password").create("instance"));
        
        super.addOption(OptionBuilder.withArgName("\"ingest\"|\"loader\"").hasArg().withDescription("Type of metrics being ingested").create("type"));
        
        super.addOption(OptionBuilder.withArgName("HDFS directory").hasArg().withDescription("Source of input files to MR job").create("input"));
        
        super.addOption(OptionBuilder.withArgName("Path to metrics.xml").hasArg().withDescription("Path to the metrics.xml configuration file.")
                        .create("conf"));
        
        super.addOption(OptionBuilder.withArgName("Path to hdfs-site.xml").hasArg().withDescription("Path to the HDFS Configuration file").create("hdfsConf"));
        
        super.addOption(OptionBuilder.withArgName("Path to webapp war file.").hasArg().withDescription("Path to the metrics web app war file.").create("war"));
        
        super.addOption(OptionBuilder.withArgName("Start date to begin processing metrics.").hasArg()
                        .withDescription("Start date to begin processing metrics, as yyyyMMddHHmm or yyyyMMdd.").create("start"));
        
        super.addOption(OptionBuilder.withArgName("Stop date for processing metrics.").hasArg()
                        .withDescription("Stop date for processing metrics, as yyyyMMddHHmm or yyyyMMdd.").create("end"));
        
        super.addOption(OptionBuilder.withArgName("Ignore flag maker metrics for correlation").hasArg()
                        .withDescription("Ignore flag maker metrics for correlation").create("ignoreFlagMakerMetrics"));
    }
    
}
