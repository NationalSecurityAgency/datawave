package nsa.datawave.metrics.web.stats;

import java.io.IOException;

import nsa.datawave.metrics.web.CloudContext;
import org.apache.hadoop.fs.FsStatus;
import org.apache.log4j.Logger;

/**
 * Produces the HDFS maximum size.
 * 
 */
public class HdfsMax extends HdfsUsage {
    private static final Logger log = Logger.getLogger(HdfsMax.class);
    
    public HdfsMax(CloudContext ctx) {
        super(ctx);
    }
    
    @Override
    protected long getValue(FsStatus fstat) throws IOException {
        return fstat.getCapacity();
    }
    
}
