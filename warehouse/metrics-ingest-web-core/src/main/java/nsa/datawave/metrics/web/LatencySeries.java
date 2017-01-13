package nsa.datawave.metrics.web;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.analytic.LongArrayWritable;
import nsa.datawave.metrics.iterators.AnalyticIngestTypeFilter;
import nsa.datawave.metrics.keys.AnalyticEntryKey;
import nsa.datawave.metrics.keys.InvalidKeyException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.LongWritable;

import com.google.gson.JsonPrimitive;

@WebServlet("services/latencySeries")
public class LatencySeries extends MetricsServlet {
    private static final long serialVersionUID = 1L;
    
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
        
        resp.setContentType("text/csv");
        Writer out = resp.getWriter();
        out.append("Timestamp,DataType,IngestType,EventCount,PollerLatency,IngestDelay,IngestLatency").append(
                        ingestType.equals("bulk") ? ",LoaderDelay,LoaderLatency" : "");
        for (Entry<Key,Value> entry : scan) {
            String keyInfo = convertKey(entry.getKey());
            String valueInfo = convertTuple(entry.getValue());
            out.append(keyInfo).append(',').append(valueInfo).append('\n');
        }
    }
    
    static String convertKey(Key key) throws IOException {
        AnalyticEntryKey aek = new AnalyticEntryKey();
        try {
            aek.parse(key);
        } catch (InvalidKeyException e) {
            throw new IOException(e);
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        
        StringBuilder buf = new StringBuilder(64);
        buf.append(sdf.format(new Date(aek.getTimestamp()))).append(',');
        buf.append(new JsonPrimitive(aek.getDataType())).append(',');
        buf.append(new JsonPrimitive(aek.getIngestType()));
        
        return buf.toString();
    }
    
    static String convertTuple(Value value) throws IOException {
        LongArrayWritable law = new LongArrayWritable();
        law.readFields(new DataInputStream(new ByteArrayInputStream(value.get())));
        StringBuilder buf = new StringBuilder(64);
        for (LongWritable l : law.convert()) {
            buf.append(l.get()).append(',');
        }
        buf.setLength(buf.length() - 1);
        return buf.toString();
    }
}
