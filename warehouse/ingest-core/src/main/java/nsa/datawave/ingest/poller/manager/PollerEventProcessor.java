package nsa.datawave.ingest.poller.manager;

import java.util.Collection;

import nsa.datawave.ingest.data.RawRecordContainer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

/**
 * Object that will be instantiated by the Poller and will be used process Events as they are read out of the raw files. The process method must be thread safe,
 * non-blocking, and fail-fast so that we don't slow down the pollers
 */
public interface PollerEventProcessor {
    
    /**
     * 
     * @return configuration options for this processor
     */
    public Collection<Option> getConfigurationOptions();
    
    /**
     * Parse command-line options and configure this processor
     * 
     * @param cl
     *            the command-line from which to retrieve arguments
     * @param config
     *            the configuration object containing other related configuration
     * @throws Exception
     *             if there is any problem configuring this processor
     */
    public void configure(CommandLine cl, Configuration config) throws Exception;
    
    /**
     * Process the RawRecordContainer object. This method *must* be thread-safe, non-blocking, and fail-fast.
     * 
     * @param e
     */
    public void process(RawRecordContainer e);
    
    /**
     * Indicates that the poller has finished it's current input file. It is possible the poller is combining many input files into a single output file. This
     * method is called whenever an input file is finished and has nothing to do with when the poller generates an output file.
     */
    public void finishFile();
    
    /**
     * stop processing and release resources
     */
    public void close();
}
