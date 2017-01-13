package nsa.datawave.poller;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Comparator;

import nsa.datawave.poller.filter.ConfigurableFilenameFilter;
import nsa.datawave.poller.filter.DatatypeAwareFilenameFilter;
import nsa.datawave.poller.manager.ConfiguredPollManager;
import nsa.datawave.poller.manager.MultiThreadedConfiguredPollManager;
import nsa.datawave.poller.manager.RecoverablePollManager;
import nsa.datawave.poller.util.PollerUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.sadun.util.polling.DirectoryPoller;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Implementation of DirectoryPoller that works with instances of {@link ConfiguredPollManager} classes.
 * 
 * This poller watches the directories specified with '-d' and moves them to a queue directory '-q'. After being moved, the fileFound method is called on the
 * ConfiguredPollManager for each file that has been moved to the queue directory. See the docs for JPoller 1.5.1 for more information. It is not the
 * responsibility of this class to clean up the queue directory.
 * 
 */
public class Poller extends DirectoryPoller {
    
    /**
     * Shutdown hook to stop the poller when the JVM shuts down.
     * 
     */
    public static class ShutdownHook extends Thread {
        
        private final DirectoryPoller p;
        
        public ShutdownHook(DirectoryPoller p) {
            this.p = p;
        }
        
        @Override
        public void run() {
            log.info("Shutting down");
            p.shutdown();
            log.info("Shut down completed successfully");
            NDC.pop();
        }
        
    }
    
    private static final Logger log = Logger.getLogger(Poller.class);
    private volatile ConfiguredPollManager pm = null;
    
    public Poller() {
        super();
    }
    
    /**
     * This method is called from the jvm shutdown hook
     */
    @Override
    public void shutdown() {
        super.shutdown();
        try {
            if (pm != null) {
                pm.close();
            }
        } catch (IOException e) {
            log.error("Error closing poll manager", e);
        }
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getOptionValue returns.
     * 
     * @param cl
     * @param opt
     * @return The last option value for opt
     */
    protected String getLastOptionValue(CommandLine cl, String opt) {
        return PollerUtils.getLastOptionValue(cl, opt);
    }
    
    /**
     * The arguments are parsed, the poller is configured and started.
     * 
     * @param args
     * @throws Exception
     */
    public void poll(String... args) throws Exception {
        
        // Instantiate the poll manager so that we can get the configuration parameters
        // from it.
        // set the poll manager
        String classname = null;
        int managerCount = 1;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-m")) {
                classname = args[i + 1];
            } else if (args[i].equals("-mc")) {
                managerCount = Integer.parseInt(args[i + 1]);
            }
        }
        if (null == classname) {
            throw new IllegalArgumentException("-m parameter must exist with valid class name");
        }
        if (managerCount < 1) {
            throw new RuntimeException("Must specify a value greater than 0 when specifying manager count");
        }
        
        Options pollManagerOptions = null;
        if (managerCount > 1) {
            pm = new MultiThreadedConfiguredPollManager(managerCount, classname);
        } else {
            Class<?> c = Class.forName(classname);
            Object o = c.newInstance();
            if (o instanceof ConfiguredPollManager) {
                pm = (ConfiguredPollManager) o;
                pm.setThreadIndex(0);
            } else {
                throw new RuntimeException("Class " + c.getName() + " is not an instance of ConfiguredPollManager");
            }
        }
        pollManagerOptions = pm.getConfigurationOptions();
        
        // Now that we have the parameters for the poll manager, add the ones required for this poller.
        
        Option directories = new Option("d", "directories", true, "comma separated list of directories to poll");
        directories.setRequired(true);
        directories.setType(String.class);
        directories.setValueSeparator(',');
        pollManagerOptions.addOption(directories);
        
        Option impl = new Option("m", "manager", true, "poll manager implementation class name");
        impl.setRequired(true);
        impl.setArgs(1);
        impl.setType(String.class);
        pollManagerOptions.addOption(impl);
        
        Option count = new Option("mc", "managerCount", true, "the number of manager instances to run concurrently (default is 1)");
        count.setRequired(false);
        count.setArgs(1);
        count.setType(Integer.class);
        pollManagerOptions.addOption(count);
        
        Option verbose = new Option("v", "verbose", false, "be verbose");
        verbose.setRequired(false);
        pollManagerOptions.addOption(verbose);
        
        Option comparator = new Option("c", "comparator", true, "Comparator class to use for ordering the processing of files");
        comparator.setRequired(false);
        comparator.setArgs(1);
        comparator.setType(String.class);
        pollManagerOptions.addOption(comparator);
        
        Option queueDirs = new Option("q", "queue", true, "comma separated list of directories to place the in-process files");
        queueDirs.setRequired(true);
        queueDirs.setType(String.class);
        queueDirs.setValueSeparator(',');
        pollManagerOptions.addOption(queueDirs);
        
        Option pollInterval = new Option("i", "interval", true, "number of ms between each poll");
        pollInterval.setRequired(true);
        pollInterval.setType(Long.class);
        pollInterval.setArgs(1);
        pollManagerOptions.addOption(pollInterval);
        
        Option filter = new Option("f", "filter", true, "FilenameFilter implementation class name");
        filter.setRequired(false);
        filter.setArgs(1);
        filter.setType(String.class);
        pollManagerOptions.addOption(filter);
        
        Option conf = new Option("fc", "filterConfig", true, "FilenameFilter configuration");
        conf.setRequired(false);
        conf.setArgs(1);
        conf.setType(String.class);
        pollManagerOptions.addOption(conf);
        
        CommandLine cl = null;
        try {
            cl = new BasicParser().parse(pollManagerOptions, args);
        } catch (ParseException pe) {
            new HelpFormatter().printHelp("Poller ", pollManagerOptions, true);
            throw pe;
        }
        
        // Configure the poller
        this.setSendSingleFileEvent(true);
        this.setName("DirectoryPoller");
        this.setTimeBased(true);
        this.setPollingTimeBased(false);
        this.setPollInterval(Long.parseLong(getLastOptionValue(cl, pollInterval.getOpt())));
        this.setAutoMove(true);
        
        // configure the poll manager(s) and apply it.
        pm.configure(cl);
        this.addPollManager(pm);
        
        int i = 0;
        String[] moveDirs = cl.getOptionValues(queueDirs.getOpt());
        for (String dir : cl.getOptionValues(directories.getOpt())) {
            File d = new File(dir);
            File m = null;
            try {
                m = new File(moveDirs[i]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new RuntimeException("Queue directory not specified for input directory " + dir);
            }
            if (!m.exists()) {
                m.mkdirs();
            }
            
            // if we can read from the polling directory, and write to the queue directory, then we add it to the list to poll
            if (d.canRead() && m.canWrite()) {
                
                // if the poll manager can perform and recovery work (e.g. move files back into the queue), then do it
                if (pm instanceof RecoverablePollManager) {
                    log.info("Calling recoverable poll manager " + pm.getClass());
                    ((RecoverablePollManager) pm).recover(m);
                }
                
                // attempt to clean up any files left in the queue directory by moving them back to the polling directory
                int succ = 0;
                int fail = 0;
                for (File queueFile : m.listFiles()) {
                    try {
                        if (!queueFile.renameTo(new File(d, queueFile.getName()))) {
                            log.error("Unable to move old queue file " + queueFile + " back into " + d);
                            fail++;
                        } else {
                            succ++;
                        }
                    } catch (Exception e) {
                        log.error("Unable to move old queue file " + queueFile + " back into " + d, e);
                        fail++;
                    }
                }
                if (succ > 0) {
                    log.info("Cleaned up " + succ + " files left in queue directory " + m);
                }
                if (fail > 0) {
                    log.error("Failed to clean up " + fail + " files left in queue directory " + m);
                }
                
                // Add the directories to poll
                this.addDirectory(d);
                
                // Associate the directories to move with each directory to poll
                this.setAutoMoveDirectory(d, m);
            } else {
                throw new RuntimeException("Unable to read directory " + dir + " or unable to write to directory " + moveDirs[i]);
            }
            i++;
        }
        
        if (cl.hasOption(verbose.getOpt()))
            this.setVerbose(true);
        
        // Set the comparator for ordering the files
        if (cl.hasOption(comparator.getOpt())) {
            Class<?> cc = Class.forName(getLastOptionValue(cl, comparator.getOpt()));
            Object co = cc.newInstance();
            if (co instanceof Comparator<?>) {
                Comparator<?> comp = (Comparator<?>) co;
                this.setFilesSortComparator(comp);
            } else {
                throw new RuntimeException("Class " + cc.getName() + " is not an instance of Comparator");
            }
        }
        
        // Set the filter
        if (cl.hasOption(filter.getOpt())) {
            Class<?> f = Class.forName(getLastOptionValue(cl, filter.getOpt()));
            boolean initialized = false;
            Object fo = null;
            for (Constructor<?> constructor : f.getConstructors()) {
                if (constructor.getParameterTypes().length == 0) {
                    fo = f.newInstance();
                } else if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0].equals(String.class)) {
                    fo = constructor.newInstance(new Object[] {pm.getDatatype()});
                    initialized = true;
                }
            }
            if (fo instanceof FilenameFilter) {
                FilenameFilter fnf = (FilenameFilter) fo;
                if (fnf instanceof DatatypeAwareFilenameFilter) {
                    DatatypeAwareFilenameFilter daff = (DatatypeAwareFilenameFilter) fnf;
                    daff.setDatatype(pm.getDatatype());
                    initialized = true;
                }
                if (!initialized) {
                    throw new RuntimeException("Class " + f.getName() + " was not initialized with the datatype");
                }
                
                if (fo instanceof ConfigurableFilenameFilter && cl.hasOption(conf.getOpt())) {
                    Configuration config = new Configuration();
                    config.addResource(getLastOptionValue(cl, conf.getOpt()));
                    ((ConfigurableFilenameFilter) fo).setConfiguration(config);
                }
                
                this.setFilter(fnf);
            } else {
                throw new RuntimeException("Class " + f.getName() + " is not an instance of FilenameFilter");
            }
            
        }
        
        NDC.push("Poller #" + this.hashCode());
        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(this));
        log.info("Starting poll");
        this.start();
    }
    
    public static Object setupMarkingLoader() {
        return null;
    }
    
    public static void main(String[] args) throws Exception {
        
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("poller-spring.xml");
        Poller p = new Poller();
        p.poll(args);
        
    }
    
}
