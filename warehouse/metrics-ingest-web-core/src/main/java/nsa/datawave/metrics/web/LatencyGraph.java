package nsa.datawave.metrics.web;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.analytic.MetricsDataFormat.MetricsField;
import nsa.datawave.metrics.iterators.AnalyticIngestTypeFilter;
import nsa.datawave.metrics.web.util.MetricsSummary;
import nsa.datawave.metrics.web.util.MetricsTableScanner;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.Pair;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Returns a JSON object that contains data type specific information on latency. The latencies are broken down into categories based on the ingest process (ie,
 * poller, ingest, loader).
 * 
 */
@WebServlet("services/latency")
public class LatencyGraph extends MetricsServlet {
    private static final long serialVersionUID = -5919892120575274118L;
    private static final Logger log = Logger.getLogger(LatencyGraph.class);
    
    private static final JsonObject BlankLabel;
    
    private static final double MINUTES = (double) TimeUnit.MINUTES.toMillis(1);
    
    @Inject
    @ConfigProperty(name = "dw.metrics.ingest.live.threshold", defaultValue = "0")
    private long liveThreshold;
    @Inject
    @ConfigProperty(name = "dw.metrics.ingest.bulk.threshold", defaultValue = "0")
    private long bulkThreshold;
    
    static {
        BlankLabel = new JsonObject();
        BlankLabel.addProperty("label", "");
    }
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Pair<String,String> timeInfo = getStartAndEnd(req);
        String start = timeInfo.getFirst();
        String end = timeInfo.getSecond();
        
        Scanner scan = ctx.createScanner();
        scan.setRange(new Range(start, end));
        
        IteratorSetting cfg = new IteratorSetting(1, AnalyticIngestTypeFilter.class);
        String ingestType = getArg("ingestType", "bulk", req);
        cfg.addOption("type", ingestType);
        scan.addScanIterator(cfg);
        
        Map<String,MetricsSummary> typeSummaries = new MetricsTableScanner().scan(scan);
        
        Map<MetricsField,JsonArray> typeTimingData = new TreeMap<>();
        JsonArray types = new JsonArray();
        for (Entry<String,MetricsSummary> summary : typeSummaries.entrySet()) {
            String type = summary.getKey();
            
            if (summary.getValue().size() == 0) {
                continue;
            }
            
            types.add(new JsonPrimitive(type));
            
            Map<MetricsField,Number> stats = "live".equalsIgnoreCase(ingestType) ? summary.getValue().getLiveSummary() : summary.getValue().getBulkSummary();
            
            for (Entry<MetricsField,Number> stat : stats.entrySet()) {
                // here is were the time values are normalized down to minutes
                getJsonArray(stat.getKey(), typeTimingData).add(new JsonPrimitive(convertToMinutes(stat.getValue().doubleValue())));
                
            }
        }
        
        // I'm focused on latency so event counts aren't necessary
        typeTimingData.remove(MetricsField.EVENT_COUNT);
        
        JsonArray seriesContainer = new JsonArray();
        JsonArray seriesLabels = new JsonArray();
        
        for (Entry<MetricsField,JsonArray> series : typeTimingData.entrySet()) {
            seriesContainer.add(series.getValue());
            JsonObject label = new JsonObject();
            label.addProperty("label", series.getKey().toString());
            seriesLabels.add(label);
        }
        
        JsonObject retVal = new JsonObject();
        retVal.addProperty("threshold", "live".equalsIgnoreCase(ingestType) ? liveThreshold : bulkThreshold);
        retVal.add("series", seriesContainer);
        retVal.add("labels", seriesLabels);
        retVal.add("types", types);
        
        resp.setContentType("application/json");
        resp.getWriter().write(retVal.toString());
    }
    
    private JsonArray getJsonArray(MetricsField type, Map<MetricsField,JsonArray> cache) {
        JsonArray array = cache.get(type);
        if (array == null) {
            array = new JsonArray();
            cache.put(type, array);
        }
        return array;
    }
    
    static double convertToMinutes(double t) {
        return t / MINUTES;
    }
}
