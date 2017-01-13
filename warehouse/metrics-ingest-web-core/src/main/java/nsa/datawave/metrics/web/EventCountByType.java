package nsa.datawave.metrics.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.iterators.DataTypeFilter;
import nsa.datawave.metrics.iterators.IngestTypeFilter;
import nsa.datawave.metrics.keys.IngestEntryKey;
import nsa.datawave.metrics.keys.InvalidKeyException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

@WebServlet("services/typeDistribution")
public class EventCountByType extends MetricsServlet {
    private static final long serialVersionUID = -1777153867026399186L;
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(EventCountByType.class);
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Pair<String,String> timeInfo = getStartAndEnd(req);
        String start = timeInfo.getFirst();
        String end = timeInfo.getSecond();
        
        Scanner scan = ctx.createIngestScanner();
        
        scan.setRange(new Range(start, end));
        
        scan.addScanIterator(new IteratorSetting(21, org.apache.accumulo.core.iterators.SortedKeyIterator.class));
        IteratorSetting cfg = new IteratorSetting(22, DataTypeFilter.class);
        scan.addScanIterator(cfg);
        cfg = new IteratorSetting(23, IngestTypeFilter.class);
        cfg.addOption("type", getArg("ingestType", "bulk", req));
        scan.addScanIterator(cfg);
        
        Map<String,AtomicLong> typeCounts = initMap();
        for (Entry<Key,Value> kv : scan) {
            try {
                IngestEntryKey key = new IngestEntryKey(kv.getKey());
                getValue(typeCounts, key.getType()).addAndGet(key.getCount());
            } catch (InvalidKeyException ex) {
                log.info("Could not parse key within IngestEntryKey", ex);
            }
            
        }
        
        JsonArray data = new JsonArray();
        for (Entry<String,AtomicLong> count : typeCounts.entrySet()) {
            JsonArray entry = new JsonArray();
            entry.add(new JsonPrimitive(count.getKey()));
            entry.add(new JsonPrimitive(count.getValue()));
            data.add(entry);
        }
        
        resp.setContentType("application/json");
        resp.getWriter().write(data.toString());
    }
    
    private Map<String,AtomicLong> initMap() {
        HashMap<String,AtomicLong> map = new HashMap<>();
        return map;
    }
    
    private AtomicLong getValue(Map<String,AtomicLong> map, String s) {
        AtomicLong al = map.get(s);
        if (al == null) {
            al = new AtomicLong();
            map.put(s, al);
        }
        return al;
    }
    
}
