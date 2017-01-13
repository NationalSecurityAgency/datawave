package nsa.datawave.metrics.web.stats;

import nsa.datawave.metrics.web.CloudContext;
import nsa.datawave.metrics.web.stats.queries.ServerQueryMetrics;
import nsa.datawave.metrics.web.stats.rfile.RFileSizeStat;

import org.apache.hadoop.conf.Configuration;

public enum MetricsStats {
    
    HDFSUSAGE, HDFSMAX, RFILESIZE, INGESTRATE, QUERYRATE, TABLESIZE, LOADAVERAGE;
    
    public static Statistic newInstance(String type, CloudContext ctx) {
        if (valueOf(type.toUpperCase()) == null)
            return null;
        return newInstance(valueOf(type.toUpperCase()), ctx);
    }
    
    public static Statistic newInstance(MetricsStats type, CloudContext ctx) {
        switch (type) {
            case HDFSUSAGE:
                return new HdfsUsage(ctx);
            case INGESTRATE:
                return new IngestRate(ctx);
            case HDFSMAX:
                return new HdfsMax(ctx);
            case QUERYRATE:
                return new QueryRate(ctx);
            case TABLESIZE:
                return new ServerQueryMetrics(ctx);
            case RFILESIZE:
                return new RFileSizeStat(ctx);
        }
        
        return null;
    }
    
}
