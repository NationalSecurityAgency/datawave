package nsa.datawave.metrics.web;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.apache.accumulo.core.iterators.SortedKeyIterator;
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

@WebServlet("services/dataflowSummary")
public class DataFlowSummaryServlet extends JobSummaryServlet {
    private static final long serialVersionUID = 8445659958315951020L;
    private static final Logger log = Logger.getLogger(DataFlowSummaryServlet.class);
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        
        Pair<String,String> timeInfo = getStartAndEnd(req);
        String start = timeInfo.getFirst();
        String end = timeInfo.getSecond();
        
        Scanner scan = ctx.createIngestScanner();
        
        scan.setRange(new Range(start, end));
        
        scan.addScanIterator(new IteratorSetting(21, SortedKeyIterator.class));
        
        IteratorSetting filter = new IteratorSetting(22, IngestTypeFilter.class);
        filter.addOption("type", getArg("ingestType", "bulk", req));
        scan.addScanIterator(filter);
        
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
        
        Calendar cal = new GregorianCalendar();
        FlowSummary summary = new FlowSummary();
        Set<Long> times = new HashSet<>();
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
                
                long endTime = getDayFromCounters(counters, IngestProcess.END_TIME);
                
                // Truncate the Date down to the day at midnight
                Date endDate = new Date(endTime);
                cal.setTime(endDate);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                endTime = cal.getTime().getTime();
                
                times.add(endTime);
                
                CounterGroup processedEventsCounters = counters.getGroup(IngestOutput.EVENTS_PROCESSED.name());
                
                for (Counter c : processedEventsCounters) {
                    String datatype = c.getDisplayName();
                    
                    if (!summary.containsKey(datatype)) {
                        summary.put(datatype, new DatatypeFlowSummary());
                    }
                    
                    DatatypeFlowSummary dtfSummary = summary.get(datatype);
                    
                    if (dtfSummary.containsKey(endTime)) {
                        dtfSummary.get(endTime).getAndAdd(c.getValue());
                    } else {
                        dtfSummary.put(endTime, new AtomicLong(c.getValue()));
                    }
                }
            }
        }
        
        // Make sure we have a count of 0 for all days, not just missing the key-value
        for (Long day : times) {
            for (DatatypeFlowSummary dtfSummary : summary.values()) {
                if (!dtfSummary.containsKey(day)) {
                    dtfSummary.put(day, new AtomicLong(0));
                }
            }
        }
        
        Gson gson = new GsonBuilder().registerTypeAdapter(FlowSummary.class, new FlowSummarySerializer()).create();
        
        String retVal = gson.toJson(summary);
        
        if (log.isDebugEnabled()) {
            log.debug("Returning:\n" + retVal);
        }
        
        resp.getWriter().write(retVal);
    }
    
    private long getDayFromCounters(Counters counters, Enum<?> key) {
        return counters.findCounter(key).getValue();
    }
    
    /*
     * Convenience classes to model hierarchy of daily statistics.
     */
    private class FlowSummary extends HashMap<String,DatatypeFlowSummary> {
        private static final long serialVersionUID = 6620381373244346136L;
    }
    
    private class DatatypeFlowSummary extends TreeMap<Long,AtomicLong> {
        private static final long serialVersionUID = -3898516208598851436L;
    }
    
    /**
     * Build the following
     * 
     * <pre>
     * [
     *  {
     *    'key': datatype,
     *    'values': [ [longTimestamp, hitcount], ... ]
     *  },
     *  
     *  {
     *    'key': datatype2,
     *    'values': [ ... ]
     *  }
     * ]
     * </pre>
     * 
     */
    private class FlowSummarySerializer implements JsonSerializer<FlowSummary> {
        @Override
        public JsonElement serialize(FlowSummary summary, Type t, JsonSerializationContext c) {
            JsonArray array = new JsonArray();
            
            for (Entry<String,DatatypeFlowSummary> entry : summary.entrySet()) {
                JsonObject hash = new JsonObject();
                
                hash.add("key", new JsonPrimitive(entry.getKey()));
                
                JsonArray values = new JsonArray();
                for (Entry<Long,AtomicLong> counts : entry.getValue().entrySet()) {
                    JsonArray pair = new JsonArray();
                    
                    pair.add(new JsonPrimitive(counts.getKey()));
                    pair.add(new JsonPrimitive(counts.getValue().get()));
                    
                    values.add(pair);
                }
                
                hash.add("values", values);
                
                array.add(hash);
            }
            
            return array;
        }
    }
}
