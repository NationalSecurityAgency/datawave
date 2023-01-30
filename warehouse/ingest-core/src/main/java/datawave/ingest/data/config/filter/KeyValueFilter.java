package datawave.ingest.data.config.filter;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.mapreduce.job.writer.ChainedContextWriter;

public interface KeyValueFilter<OK,OV> extends ChainedContextWriter<OK,OV> {
    
    /**
     * Return the list of tables that are used by this filter. Note that the handler should NOT have to be "setup" to call this method.
     * 
     * @param conf
     *            the configuration to check
     * @return list of tables
     */
    String[] getTableNames(Configuration conf);
    
    /**
     * Return the list of table priorities that are used by this handler. Note that the handler should NOT have to be "setup" to call this method.
     * 
     * @param conf
     *            the configuration to check
     * @return list of table priorities
     */
    int[] getTableLoaderPriorities(Configuration conf);
    
}
