package nsa.datawave.metrics.web.stats;

import nsa.datawave.metrics.web.CloudContext;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Purpose: Base class for all system statistics. Justification: most base functionality is included here, including gathering the username/password, and the
 * zookeeper information
 * 
 */
public abstract class Statistic {
    
    protected CloudContext ctx;
    
    public Statistic(CloudContext ctx) {
        this.ctx = ctx;
    }
    
    public abstract void setMasterStats(MasterMonitorInfo masterStats);
    
    public abstract com.google.gson.JsonElement toJson(HttpServletRequest req);
    
    public static TableInfo summarizeTableStats(TabletServerStatus status) {
        TableInfo summary = new TableInfo();
        summary.majors = new Compacting();
        summary.minors = new Compacting();
        for (TableInfo rates : status.tableMap.values()) {
            add(summary, rates);
        }
        return summary;
    }
    
    public static void add(TableInfo total, TableInfo more) {
        if (total.minors == null)
            total.minors = new Compacting();
        if (total.majors == null)
            total.majors = new Compacting();
        if (more.minors != null) {
            total.minors.running += more.minors.running;
            total.minors.queued += more.minors.queued;
        }
        if (more.majors != null) {
            total.majors.running += more.majors.running;
            total.majors.queued += more.majors.queued;
        }
        total.onlineTablets += more.onlineTablets;
        total.recs += more.recs;
        total.recsInMemory += more.recsInMemory;
        total.tablets += more.tablets;
        total.ingestRate += more.ingestRate;
        total.ingestByteRate += more.ingestByteRate;
        total.queryRate += more.queryRate;
        total.queryByteRate += more.queryByteRate;
    }
    
    public static Map<String,Double> summarizeTableStats(MasterMonitorInfo mmi) {
        Map<String,Double> compactingByTable = new HashMap<>();
        if (mmi != null && mmi.tServerInfo != null) {
            for (TabletServerStatus status : mmi.tServerInfo) {
                if (status != null && status.tableMap != null) {
                    for (String table : status.tableMap.keySet()) {
                        Double holdTime = compactingByTable.get(table);
                        compactingByTable.put(table, Math.max(holdTime == null ? 0. : holdTime, status.holdTime));
                    }
                }
            }
        }
        return compactingByTable;
    }
    
}
