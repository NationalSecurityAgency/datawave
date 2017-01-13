package nsa.datawave.metrics.web;

import java.io.IOException;
import java.util.Map.Entry;

import javax.annotation.security.RunAs;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.iterators.DataTypeFilter;
import nsa.datawave.metrics.iterators.IngestTypeFilter;
import nsa.datawave.metrics.util.WritableUtil;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

@RunAs("InternalUser")
@WebServlet("services/irs")
public class IngestRateSeries extends MetricsServlet {
    private static final long serialVersionUID = 6775418888217845228L;
    
    @Override
    public void doGet(CloudContext cxn, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Pair<String,String> timeInfo = getStartAndEnd(req);
        String start = timeInfo.getFirst();
        String end = timeInfo.getSecond();
        
        Scanner scan = cxn.createIngestScanner();
        
        scan.setRange(new Range(start, end));
        
        scan.addScanIterator(new IteratorSetting(1, org.apache.accumulo.core.iterators.SortedKeyIterator.class));
        IteratorSetting cfg = new IteratorSetting(2, DataTypeFilter.class);
        cfg.addOption("types", getArg("dataTypes", "", req));
        scan.addScanIterator(cfg);
        cfg = new IteratorSetting(2, IngestTypeFilter.class);
        cfg.addOption("type", getArg("ingestType", "bulk", req));
        scan.addScanIterator(cfg);
        scan.addScanIterator(new IteratorSetting(3, nsa.datawave.metrics.iterators.IngestRateIterator.class));
        
        JsonArray series = new JsonArray();
        try {
            for (Entry<Key,Value> e : scan) {
                JsonArray seriesEntry = new JsonArray();
                long timestamp = WritableUtil.parseLong(e.getKey().getRow());
                seriesEntry.add(new JsonPrimitive(timestamp));
                seriesEntry.add(new JsonPrimitive(Double.parseDouble(e.getKey().getColumnFamily().toString())));
                series.add(seriesEntry);
            }
        } catch (Exception e) {
            // do nothing
        }
        
        // if we have no data, set the bounds on the data
        if (series.size() == 0) {
            JsonArray begin = new JsonArray();
            begin.add(new JsonPrimitive(Long.parseLong(start)));
            begin.add(new JsonPrimitive(0));
            series.add(begin);
            
            JsonArray stop = new JsonArray();
            stop.add(new JsonPrimitive(Long.parseLong(end)));
            stop.add(new JsonPrimitive(0));
            series.add(stop);
        }
        
        resp.setContentType("application/json");
        resp.getWriter().write(series.toString());
    }
}
