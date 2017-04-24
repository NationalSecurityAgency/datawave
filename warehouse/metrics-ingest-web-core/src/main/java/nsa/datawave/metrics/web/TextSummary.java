package nsa.datawave.metrics.web;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Returns a JSON Array of objects that represent the phases of ingest back to the client. The order of objects in the array map to the order of the process run
 * by ingest: <blockquote> 1- Poller Processing 2- Sequence File on HDFS 3- Ingest Processing
 * 
 * If live: 4- Total Latency
 * 
 * If bulk: 4- RFile on HDFS 5- Loader process Else: You did it wrong. </blockquote>
 * 
 * Each object contains a mapping of data type to formatted latency for that particular piece. It is formatted for easy display.
 */
@WebServlet("services/textSummary")
public class TextSummary extends MetricsServlet {
    private static final long serialVersionUID = 1266878635393024359L;
    
    private static final JsonArray AA_COLUMNS_L, AA_COLUMNS_B;
    
    static {
        AA_COLUMNS_L = new JsonArray();
        AA_COLUMNS_L.add(makeSTitle("Data Type"));
        AA_COLUMNS_L.add(makeSTitle("Poller Processing"));
        AA_COLUMNS_L.add(makeSTitle("Sequence File On HDFS"));
        AA_COLUMNS_L.add(makeSTitle("Ingest Processing"));
        AA_COLUMNS_L.add(makeSTitle("Total Latency"));
        
        AA_COLUMNS_B = new JsonArray();
        AA_COLUMNS_B.add(makeSTitle("Data Type"));
        AA_COLUMNS_B.add(makeSTitle("Poller Processing"));
        AA_COLUMNS_B.add(makeSTitle("Sequence File On HDFS"));
        AA_COLUMNS_B.add(makeSTitle("Ingest Processing"));
        AA_COLUMNS_B.add(makeSTitle("RFiles on HDFS"));
        AA_COLUMNS_B.add(makeSTitle("Loader Processing"));
        AA_COLUMNS_B.add(makeSTitle("Total Latency"));
    }
    
    private static JsonObject makeSTitle(String title) {
        JsonObject o = new JsonObject();
        o.addProperty("sTitle", title);
        return o;
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
        boolean isLive = "live".equalsIgnoreCase(ingestType);
        
        String dataTypes = getArg("dataTypes", "", req);
        HashSet<String> dataTypeFilter;
        if (dataTypes.isEmpty()) {
            dataTypeFilter = new HashSet<String>(1) {
                private static final long serialVersionUID = 1L;
                
                @Override
                public boolean contains(Object o) {
                    return true;
                }
            };
        } else {
            dataTypeFilter = new HashSet<>();
            Collections.addAll(dataTypeFilter, dataTypes.split(","));
        }
        
        cfg.addOption("type", ingestType);
        scan.addScanIterator(cfg);
        
        Map<String,MetricsSummary> typeSummaries = new MetricsTableScanner().scan(scan);
        
        resp.setContentType("application/json");
        resp.getWriter().write(getTimeSpans(typeSummaries, isLive, dataTypeFilter));
    }
    
    public String getTimeSpans(Map<String,MetricsSummary> typeSummaries, boolean isLive, Collection<String> dataTypeFilter) {
        JsonArray aaData = new JsonArray();
        for (Entry<String,MetricsSummary> summary : typeSummaries.entrySet()) {
            if (!dataTypeFilter.contains(summary.getKey()) || summary.getValue().size() == 0) {
                continue;
            }
            aaData.add(populateTimeSpans(summary.getKey(), summary.getValue(), isLive));
        }
        
        JsonObject obj = new JsonObject();
        obj.add("aaData", aaData);
        obj.add("aoColumns", isLive ? AA_COLUMNS_L : AA_COLUMNS_B);
        return new Gson().toJson(obj);
    }
    
    /**
     * Scans over both a set of <code>TimeSpan</code> objects and a <code>MetricsSummary</code> to build up a data type to latency mapping for each time span.
     * 
     * @param dataType
     *            the data type the summary applies to
     * @param summary
     *            the summary associated with the given data type
     * @param isLive
     *            whether or not we're doing live metrics (effects the number of times <code>next()</code> is called on <code>itr</code>
     */
    private JsonArray populateTimeSpans(String dataType, MetricsSummary summary, boolean isLive) {
        Map<MetricsField,Number> numbers;
        if (isLive) {
            numbers = summary.getLiveSummary();
        } else {
            numbers = summary.getBulkSummary();
        }
        
        JsonArray row = new JsonArray();
        row.add(new JsonPrimitive(dataType));
        
        long totalLatency = numbers.get(MetricsField.POLLER_DURATION).longValue();
        row.add(new JsonPrimitive(formatTime(numbers.get(MetricsField.POLLER_DURATION))));
        
        totalLatency += numbers.get(MetricsField.INGEST_DELAY).longValue();
        row.add(new JsonPrimitive(formatTime(numbers.get(MetricsField.INGEST_DELAY))));
        
        totalLatency += numbers.get(MetricsField.INGEST_DURATION).longValue();
        row.add(new JsonPrimitive(formatTime(numbers.get(MetricsField.INGEST_DURATION))));
        
        if (!isLive) {
            totalLatency += numbers.get(MetricsField.LOADER_DELAY).longValue();
            row.add(new JsonPrimitive(formatTime(numbers.get(MetricsField.LOADER_DELAY))));
            
            totalLatency += numbers.get(MetricsField.LOADER_DURATION).longValue();
            row.add(new JsonPrimitive(formatTime(numbers.get(MetricsField.LOADER_DURATION))));
        }
        
        row.add(new JsonPrimitive(formatTime(totalLatency)));
        
        return row;
    }
    
    private static final String[] suffixes = {"h", "m", "s"};
    
    /**
     * Turns a raw value in milliseconds into HMS format.
     * 
     * @param milliseconds
     * @return
     */
    private String formatTime(Number milliseconds) {
        long ms = milliseconds.longValue();
        
        if (ms < 1000L) {
            return ms + "ms";
        }
        
        TimeUnit[] conversions = new TimeUnit[3];
        conversions[0] = TimeUnit.HOURS;
        conversions[1] = TimeUnit.MINUTES;
        conversions[2] = TimeUnit.SECONDS;
        
        StringBuilder formatted = new StringBuilder();
        for (int i = 0; i < conversions.length; ++i) {
            long time = conversions[i].convert(ms, TimeUnit.MILLISECONDS);
            if (time != 0) {
                formatted.append(time).append(suffixes[i]).append(' ');
                ms -= conversions[i].toMillis(time);
            }
        }
        
        return formatted.toString();
    }
}
