package nsa.datawave.metrics.web.stats;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import nsa.datawave.metrics.web.CloudContext;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * Produces the JSON array to provide the current HDFS usage.
 * 
 */
public class HdfsUsage extends Statistic {
    
    private static final Logger log = Logger.getLogger(HdfsUsage.class);
    
    public HdfsUsage(CloudContext ctx) {
        super(ctx);
    }
    
    @Override
    public void setMasterStats(MasterMonitorInfo masterStats) {
        // not needed
    }
    
    @Override
    public JsonElement toJson(HttpServletRequest req) {
        JsonArray retVal = new JsonArray();
        
        String hdfsInstance = req.getParameter("hdfs");
        boolean isWarehouse = null != hdfsInstance && "warehouse".equalsIgnoreCase(hdfsInstance);
        try {
            FileSystem fs = ctx.getFileSystem(isWarehouse);
            retVal.add(new JsonPrimitive(getValue(fs.getStatus())));
        } catch (IOException e) {
            log.error(e);
        }
        
        return retVal;
        
    }
    
    protected long getValue(FsStatus fstat) throws IOException {
        return fstat.getUsed();
    }
    
}
