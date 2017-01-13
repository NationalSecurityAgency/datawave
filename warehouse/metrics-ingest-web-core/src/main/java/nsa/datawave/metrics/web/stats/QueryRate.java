package nsa.datawave.metrics.web.stats;

import javax.servlet.http.HttpServletRequest;

import nsa.datawave.metrics.web.CloudContext;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class QueryRate extends Statistic {
    
    private static final Logger log = Logger.getLogger(QueryRate.class);
    MasterMonitorInfo masterStats;
    
    public QueryRate(CloudContext ctx) {
        super(ctx);
    }
    
    @Override
    public void setMasterStats(MasterMonitorInfo masterStats) {
        this.masterStats = masterStats;
        
    }
    
    @Override
    public JsonElement toJson(HttpServletRequest req) {
        
        long queryRate = 0;
        
        try {
            
            for (TabletServerStatus server : masterStats.tServerInfo) {
                TableInfo summary = summarizeTableStats(server);
                queryRate += summary.queryByteRate;
                
            }
            
            queryRate /= masterStats.tServerInfo.size();
            
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error fetching stats: " + e);
            
        }
        
        JsonArray retVal = new JsonArray();
        retVal.add(new JsonPrimitive(Double.valueOf(queryRate)));
        return retVal;
    }
    
}
