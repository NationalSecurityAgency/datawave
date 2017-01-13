package nsa.datawave.metrics.web;

import static com.google.common.collect.Iterables.transform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.analytic.FileLatency;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.VLongWritable;

import com.google.common.base.Function;
import com.google.gson.JsonObject;

@WebServlet
@SuppressWarnings("serial")
public class PollerFileCounts extends MetricsServlet {
    @Override
    public void doGet(CloudContext connection, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Pair<String,String> times = super.getStartAndEnd(req);
        final String start = times.getFirst(), end = times.getSecond();
        final String type = super.getArg("type", "bulk", req);
        Scanner scan;
        try {
            scan = connection.createFileLatenciesScanner();
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }
        int stackStart = 30;
        IteratorSetting colQFilter = new IteratorSetting(stackStart++, RegExFilter.class);
        colQFilter.addOption(RegExFilter.COLQ_REGEX, type);
        scan.addScanIterator(colQFilter);
        scan.addScanIterator(new IteratorSetting(stackStart++, EventCountSummation.class));
        scan.setRange(new Range(start, end));
        long sum = 0L;
        for (long l : transform(scan, new EntryParser())) {
            sum += l;
        }
        JsonObject count = new JsonObject();
        count.addProperty("count", sum);
        resp.getWriter().write(count.toString());
        resp.setContentType("application/json");
    }
    
    private static class EntryParser implements Function<Entry<Key,Value>,Long> {
        public Long apply(Entry<Key,Value> e) {
            try {
                VLongWritable vlw = new VLongWritable();
                vlw.readFields(new DataInputStream(new ByteArrayInputStream(e.getValue().get())));
                return vlw.get();
            } catch (IOException e1) {
                throw new IllegalArgumentException(e1);
            }
        }
    }
    
    public static class EventCountSummation implements SortedKeyValueIterator<Key,Value> {
        private SortedKeyValueIterator<Key,Value> src;
        private Key tk;
        private Value tv;
        
        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
            this.src = source;
        }
        
        @Override
        public boolean hasTop() {
            return tk != null;
        }
        
        @Override
        public void next() throws IOException {
            tk = null;
            tv = null;
            final FileLatency fl = new FileLatency();
            long count = 0L;
            Key lastKey = null;
            while (src.hasTop()) {
                fl.readFields(new DataInputStream(new ByteArrayInputStream(src.getTopValue().get())));
                count += fl.getEventCount();
                lastKey = src.getTopKey();
                src.next();
            }
            if (lastKey != null) {
                tk = new Key(lastKey.getRow());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                new VLongWritable(count).write(new DataOutputStream(baos));
                tv = new Value(baos.toByteArray());
            }
        }
        
        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            this.src.seek(range, columnFamilies, inclusive);
            next();
        }
        
        @Override
        public Key getTopKey() {
            return tk;
        }
        
        @Override
        public Value getTopValue() {
            return tv;
        }
        
        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            EventCountSummation itr = new EventCountSummation();
            itr.src = src.deepCopy(env);
            return itr;
        }
    }
}
