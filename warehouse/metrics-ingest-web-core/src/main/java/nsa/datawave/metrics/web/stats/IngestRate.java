package nsa.datawave.metrics.web.stats;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import nsa.datawave.metrics.web.CloudContext;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;

public class IngestRate extends Statistic {
    
    private static final Logger log = Logger.getLogger(IngestRate.class);
    
    protected MasterMonitorInfo masterStats;
    
    public IngestRate(CloudContext ctx) {
        super(ctx);
    }
    
    @Override
    public void setMasterStats(MasterMonitorInfo masterStats) {
        this.masterStats = masterStats;
        
    }
    
    @Override
    public JsonElement toJson(HttpServletRequest req) {
        
        long ingestRate = 0;
        try {
            
            for (TabletServerStatus server : masterStats.tServerInfo) {
                TableInfo summary = summarizeTableStats(server);
                ingestRate += summary.ingestByteRate;
            }
            
            ingestRate /= masterStats.tServerInfo.size();
            
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error fetching stats: " + e);
            
        }
        
        JsonArray retVal = new JsonArray();
        retVal.add(new JsonPrimitive(Double.valueOf(ingestRate)));
        return retVal;
    }
    
}
