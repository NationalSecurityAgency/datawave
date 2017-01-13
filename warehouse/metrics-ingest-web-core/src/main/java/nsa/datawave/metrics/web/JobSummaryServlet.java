package nsa.datawave.metrics.web;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.ingest.metric.IngestInput;
import nsa.datawave.ingest.metric.IngestOutput;
import nsa.datawave.ingest.metric.IngestProcess;
import nsa.datawave.metrics.iterators.IngestTypeFilter;
import nsa.datawave.metrics.keys.IngestEntryKey;
import nsa.datawave.metrics.keys.InvalidKeyException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Creates a list of jobs that ran within the time series and merges their characteristics to give a summary of events procssed, including a break down by type,
 * and an overall events in and events out view.
 */
@WebServlet("services/jobSummary")
public class JobSummaryServlet extends MetricsServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(JobSummaryServlet.class);
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        
        Pair<String,String> timeInfo = getStartAndEnd(req);
        String start = timeInfo.getFirst();
        String end = timeInfo.getSecond();
        
        Scanner scan = ctx.createIngestScanner();
        
        scan.setRange(new Range(start, end));
        
        scan.addScanIterator(new IteratorSetting(21, org.apache.accumulo.core.iterators.SortedKeyIterator.class));
        IteratorSetting cfg = new IteratorSetting(22, IngestTypeFilter.class);
        cfg.addOption("type", getArg("ingestType", "bulk", req));
        scan.addScanIterator(cfg);
        
        /* First need to get a list of job id's */
        TreeSet<String> jobIds = new TreeSet<>();
        for (Entry<Key,Value> e : scan) {
            
            try {
                IngestEntryKey iek = new IngestEntryKey(e.getKey());
                jobIds.add(iek.getJobId());
                
                if (log.isDebugEnabled()) {
                    log.debug("Found job id: " + iek.getJobId());
                }
            } catch (InvalidKeyException ex) {
                log.info("Could not parse key within IngestEntryKey", ex);
            }
        }
        
        if (jobIds.isEmpty()) {
            log.debug("Could not find any job IDs.");
            resp.getWriter().write("[]");
            return;
        }
        
        /*
         * the user made a query for their timezone, but we're usually operating only in UTC. since we create the date string for the data table here, we need
         * to set the timezone on the date format.
         */
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        int timeZoneOffsetMillis = Integer.parseInt(getArg("tzOffset", "0", req)) * 60000;
        sdf.setTimeZone(new SimpleTimeZone(timeZoneOffsetMillis, ""));
        
        TreeMap<String,DailySummary> dailySummaries = new TreeMap<>();
        TreeSet<String> dataTypes = new TreeSet<>();
        scan.clearScanIterators();
        scan.setRange(new Range("jobId\u0000" + jobIds.first(), "jobId\u0000" + jobIds.last() + "~"));
        for (Entry<Key,Value> e : scan) {
            String jobId = e.getKey().getRow().toString();
            
            if (log.isDebugEnabled()) {
                log.debug("Getting counters for " + jobId);
            }
            
            if (jobIds.contains(jobId.substring("jobId\u0000".length()))) {
                Counters counters = new Counters();
                counters.readFields(new DataInputStream(new ByteArrayInputStream(e.getValue().get())));
                
                JobSummary jobSummary = new JobSummary();
                jobSummary.types = getTypeCounts(counters.getGroup(IngestOutput.EVENTS_PROCESSED.name()));
                for (TypeCount tc : jobSummary.types) {
                    dataTypes.add(tc.type);
                }
                // total # of events that didn't make it is fatal error + runime exceptions
                jobSummary.errors = counters.findCounter(IngestInput.EVENT_FATAL_ERROR).getValue();
                jobSummary.errors += counters.findCounter(IngestProcess.RUNTIME_EXCEPTION).getValue();
                
                String endDay = getDayFromCounters(sdf, counters, IngestProcess.END_TIME);
                
                DailySummary ds = dailySummaries.get(endDay);
                if (ds == null) {
                    ds = new DailySummary();
                    ds.day = endDay;
                    dailySummaries.put(endDay, ds);
                }
                ds.addJobSummary(jobSummary);
            }
        }
        
        // now I need to go through and add all of summaries add in the correct
        // number of data types
        AtomicLong empty = new AtomicLong();
        for (DailySummary ds : dailySummaries.values()) {
            for (String type : dataTypes) {
                if (!ds.typeCounts.containsKey(type)) {
                    ds.typeCounts.put(type, empty);
                }
            }
        }
        
        Gson gson = new GsonBuilder().registerTypeAdapter(DailySummary.class, new DailySummarySerializer()).create();
        
        JsonObject rData = new JsonObject();
        JsonElement dataBlob = gson.toJsonTree(dailySummaries.values());
        rData.add("aaData", dataBlob);
        
        JsonElement header = buildAoColumns(dataTypes);
        rData.add("aoColumns", header);
        
        String retVal = gson.toJson(rData);
        
        if (log.isDebugEnabled()) {
            log.debug("Returning:\n" + retVal);
        }
        
        resp.getWriter().write(retVal);
    }
    
    private TreeSet<TypeCount> getTypeCounts(CounterGroup group) {
        TreeSet<TypeCount> tcs = new TreeSet<>(new Comparator<TypeCount>() {
            @Override
            public int compare(TypeCount a, TypeCount b) {
                return a.type.compareTo(b.type);
            }
        });
        
        for (Counter c : group) {
            TypeCount tc = new TypeCount();
            tc.type = c.getDisplayName();
            tc.count = c.getValue();
            tcs.add(tc);
        }
        return tcs;
    }
    
    private String getDayFromCounters(DateFormat format, Counters counters, Enum<?> key) {
        long timestamp = counters.findCounter(key).getValue();
        Date d = new Date(timestamp);
        return format.format(d);
    }
    
    /*
     * Convenience classes to model hierarchy of daily statistics.
     */
    private class TypeCount {
        String type;
        long count;
    }
    
    private class JobSummary {
        SortedSet<TypeCount> types;
        long errors;
    }
    
    private class DailySummary {
        String day;
        TreeMap<String,AtomicLong> typeCounts = new TreeMap<>();
        long totalIn = 0;
        long errors = 0;
        
        void addJobSummary(JobSummary js) {
            for (TypeCount tc : js.types) {
                AtomicLong l = typeCounts.get(tc.type);
                if (l == null) {
                    l = new AtomicLong();
                    typeCounts.put(tc.type, l);
                }
                l.addAndGet(tc.count);
                totalIn += tc.count;
            }
            errors += js.errors;
        }
    }
    
    /**
     * Builds an aaData formated JSON array of a Daily Summary. This allows the summary to be used as a row in a DataTables formatted table.
     */
    private class DailySummarySerializer implements JsonSerializer<DailySummary> {
        @Override
        public JsonElement serialize(DailySummary ds, Type t, JsonSerializationContext c) {
            JsonArray summary = new JsonArray();
            // formats date in yyyy-MM-dd w/o a new parser
            summary.add(new JsonPrimitive(ds.day.substring(0, 4) + '-' + ds.day.substring(4, 6) + '-' + ds.day.substring(6)));
            NumberFormat nf = NumberFormat.getIntegerInstance();
            nf.setGroupingUsed(true);
            for (Entry<String,AtomicLong> tc : ds.typeCounts.entrySet()) {
                summary.add(new JsonPrimitive(nf.format(tc.getValue().get())));
            }
            summary.add(new JsonPrimitive(nf.format(ds.totalIn)));
            summary.add(new JsonPrimitive(nf.format(ds.errors)));
            summary.add(new JsonPrimitive(nf.format(ds.totalIn - ds.errors)));
            return summary;
        }
    }
    
    /**
     * Builds the 'aoColumns' portion for a DataTables table. I want to use the JSON Object feature but still need to declare up front what properties to use
     * for columns.
     */
    public static JsonElement buildAoColumns(Set<String> dataTypes) {
        JsonArray aoColumns = new JsonArray();
        aoColumns.add(createHeader("sTitle", "Date"));
        for (String dt : dataTypes) {
            aoColumns.add(createHeader("sTitle", dt));
        }
        aoColumns.add(createHeader("sTitle", "Total In"));
        aoColumns.add(createHeader("sTitle", "Errors"));
        aoColumns.add(createHeader("sTitle", "Total Out"));
        return aoColumns;
    }
    
    /**
     * Creates a simple header object for a DataTables column.
     */
    private static JsonObject createHeader(String key, String val) {
        JsonObject obj = new JsonObject();
        obj.addProperty(key, val);
        return obj;
    }
}
