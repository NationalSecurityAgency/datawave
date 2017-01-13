package nsa.datawave.metrics.web;

import java.io.IOException;
import java.util.Map.Entry;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.iterators.DataTypeFilter;
import nsa.datawave.metrics.iterators.EventCountIterator;
import nsa.datawave.metrics.iterators.IngestTypeFilter;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.Pair;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

@WebServlet("services/ecs")
public class EventCountSeries extends MetricsServlet {
    private static final long serialVersionUID = 6775418888217845228L;
    private static final Logger log = Logger.getLogger(EventCountSeries.class);
    
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
        scan.addScanIterator(new IteratorSetting(24, EventCountIterator.class));
        
        // unused values actually have a CSV of the job id's
        JsonArray series = new JsonArray();
        
        for (Entry<Key,?> tc : scan) {
            JsonArray point = new JsonArray();
            String timestamp = tc.getKey().getRow().toString();
            String count = tc.getKey().getColumnFamily().toString();
            if (timestamp.isEmpty() || count.isEmpty()) {
                log.error("Received empty time or count! time[" + timestamp + "], count[" + count + "]");
                continue;
            }
            // date
            point.add(new JsonPrimitive(Long.parseLong(timestamp)));
            // count
            point.add(new JsonPrimitive(Long.parseLong(count)));
            series.add(point);
        }
        
        resp.setContentType("application/json");
        resp.getWriter().write(series.toString());
    }
}
