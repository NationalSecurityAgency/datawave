package nsa.datawave.poller.filter;

import org.apache.hadoop.conf.Configuration;

public interface ConfigurableFilenameFilter {
    
    public void setConfiguration(Configuration conf);
    
}
