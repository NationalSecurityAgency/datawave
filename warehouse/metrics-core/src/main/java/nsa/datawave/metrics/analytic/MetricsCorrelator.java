package nsa.datawave.metrics.analytic;

import nsa.datawave.metrics.config.MetricsConfig;
import nsa.datawave.metrics.config.MetricsOptions;
import nsa.datawave.metrics.mapreduce.util.JobSetupUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.net.URL;

public class MetricsCorrelator extends Configured implements Tool {
    
    private static final Logger log = Logger.getLogger(MetricsCorrelator.class);
    
    /**
     * Currently broken wrt to configurable time ranges :(
     */
    @Override
    public int run(String[] args) throws Exception {
        configure(args);
        
        JobSetupUtil.printConfig(getConf(), log);
        
        JobFactory factory = new JobFactory();
        Job job = factory.createJob(getConf());
        job.submit();
        
        JobSetupUtil.changeJobPriority(job, log);
        
        job.waitForCompletion(true);
        
        return 0;
    }
    
    /*
     * Sets up the Configuration object
     */
    public void configure(String[] args) throws ParseException {
        Configuration conf = getConf();
        
        log.info("Searching for metrics.xml on classpath.");
        URL cpConfig = this.getClass().getClassLoader().getResource("metrics.xml");
        if (cpConfig == null) {
            log.error("No configuration file specified nor runtime args supplied- exiting.");
            System.exit(1);
        } else {
            log.info("Using conf file located at " + cpConfig.toString());
            conf.addResource(cpConfig);
        }
        
        MetricsOptions mOpts = new MetricsOptions();
        CommandLine cl = new GnuParser().parse(mOpts, args);
        // add the file config options first
        String confFiles = cl.getOptionValue("conf", "");
        if (confFiles != null && !confFiles.isEmpty()) {
            for (String confFile : confFiles.split(",")) {
                
                if (!confFile.isEmpty()) {
                    log.trace("Adding " + confFile + " to configurations resource base.");
                    conf.addResource(confFile);
                }
            }
        }
        
        // now get the runtime overrides
        for (Option opt : cl.getOptions()) {
            // Ensure we don't try to set a null value (option-only) because this will
            // cause an NPE out of Configuration/Hashtable
            conf.set(MetricsConfig.MTX + opt.getOpt(), null == opt.getValue() ? "" : opt.getValue());
        }
    }
    
    /**
     * Expects to receive args in the order of [config opts] [dates] ... where [dates] are the last two
     */
    public static void main(String[] args) {
        try {
            ToolRunner.run(new MetricsCorrelator(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
