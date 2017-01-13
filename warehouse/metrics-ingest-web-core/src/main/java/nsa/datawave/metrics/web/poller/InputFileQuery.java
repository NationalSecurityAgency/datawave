package nsa.datawave.metrics.web.poller;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.web.CloudContext;
import nsa.datawave.metrics.web.MetricsServlet;

import nsa.datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

@WebServlet
public class InputFileQuery extends MetricsServlet {
    
    private static final long serialVersionUID = 1L;
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Pair<String,String> dateBounds = super.getStartAndEnd(req);
        final String startDate = DateHelper.formatToTimeExactToSeconds(Long.parseLong(dateBounds.getFirst()));
        final String endDate = DateHelper.formatToTimeExactToSeconds(Long.parseLong(dateBounds.getSecond()));
        final String dataType = req.getParameter("type");
        
        IteratorSetting fileFilter = new IteratorSetting(21, InputFileFilter.class);
        fileFilter.addOption(InputFileFilter.START, startDate);
        fileFilter.addOption(InputFileFilter.END, endDate);
        fileFilter.addOption(InputFileFilter.TYPE, dataType);
        
        Scanner scanner;
        try {
            scanner = ctx.createPollerScanner();
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }
        scanner.addScanIterator(fileFilter);
        scanner.setRange(new Range(dataType + ".", dataType + "_\uFFFF"));
        
        Gson gson = new GsonBuilder().setPrettyPrinting().registerTypeAdapter(Counters.class, new CountersJson())
                        .registerTypeAdapter(CounterGroup.class, new CounterGroupJson()).create();
        
        resp.setContentType("application/json");
        BufferedWriter out = new BufferedWriter(resp.getWriter());
        out.append("[\n");
        for (Entry<Key,Value> entry : scanner) {
            Counters c = new Counters();
            c.readFields(new DataInputStream(new ByteArrayInputStream(entry.getValue().get())));
            out.write(gson.toJson(c));
            out.write("\n,\n");
        }
        out.append("\n]");
        out.close();
    }
    
    /**
     * Given a data type/category and a date range, this iterator makes attempts to efficiently seek to sequence files that meet the supplied criteria.
     * 
     * The datatype and date are good seeds because they occur very early in the row key in the PollerMetrics table. The category is always first. If there were
     * multiple pollers for that data type on the same node, the data type will be followed by a dot ('.') and a label indicating which poller it came from. An
     * underscore ('_') delimits the end of the category and the next field is the date. Therefore, it's easy to seek across both data types (even with multiple
     * pollers) and data types.
     */
    public static class InputFileFilter implements SortedKeyValueIterator<Key,Value> {
        public static final String START = "start", END = "end", TYPE = "type";
        SortedKeyValueIterator<Key,Value> src;
        Range scanRange;
        String startDate, endDate, type;
        Key tk;
        Value tv;
        
        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
            src = source;
            startDate = options.get(START);
            endDate = options.get(END);
            type = options.get(TYPE);
        }
        
        @Override
        public boolean hasTop() {
            return tk != null;
        }
        
        @Override
        public void next() throws IOException {
            tk = null;
            tv = null;
            while (src.hasTop()) {
                InputFile file = InputFile.parse(src.getTopKey().getRow().toString());
                final int typeComparison = type.compareTo(file.type);
                
                if (typeComparison == 0) {
                    final int startComparison = startDate.compareTo(file.date);
                    if (startComparison <= 0) {
                        final int endComparison = endDate.compareTo(file.date);
                        if (endComparison >= 0) {
                            tk = src.getTopKey();
                            tv = src.getTopValue();
                            src.next();
                            return;
                        } else {
                            if (file.hasFlowNumber()) {
                                // we need to move to the next type
                                seekToNewType(file);
                            } else {
                                // if there's not a new flow to jump to, or we're at
                                // a single flow data type, then we're done
                                return;
                            }
                        }
                    } else {
                        // we need to move up to at least the startDate of the query
                        seekToAtLeastStartDate(file);
                    }
                } else {
                    if (typeComparison > 0) {
                        // type 'foo' is less than my desired type 'zoo', so seek up to it
                        seekToDatatype();
                    } else {
                        return;
                    }
                }
            }
        }
        
        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            scanRange = range;
            src.seek(range, columnFamilies, inclusive);
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
            InputFileFilter f = new InputFileFilter();
            f.src = src.deepCopy(env);
            return f;
        }
        
        void seekToNewType(InputFile file) throws IOException {
            Key newStart = new Key(file.type + '.' + file.flowNumber + '\uFFFF');
            src.seek(new Range(newStart, false, scanRange.getEndKey(), scanRange.isEndKeyInclusive()), Collections.<ByteSequence> emptySet(), false);
        }
        
        void seekToAtLeastStartDate(InputFile file) throws IOException {
            StringBuilder newRowBuffer = new StringBuilder();
            newRowBuffer.append(file.hasFlowNumber() ? file.type + '.' + file.flowNumber : file.type).append('_').append(startDate);
            Text newRow = new Text(newRowBuffer.toString());
            Key newStart = new Key(newRow);
            src.seek(new Range(newStart, false, scanRange.getEndKey(), scanRange.isEndKeyInclusive()), Collections.<ByteSequence> emptySet(), false);
        }
        
        void seekToDatatype() throws IOException {
            src.seek(new Range(new Key(type), false, scanRange.getEndKey(), scanRange.isEndKeyInclusive()), Collections.<ByteSequence> emptySet(), false);
        }
    }
    
    /**
     * An immutable struct for representing a parsed sequence file name.
     * 
     * A sequence file name can take the form of: someDataType_20130116212757_host.name_134ce9fbf130d3400.seq -OR-
     * someDataType.1_20130116212757_host.name_134ce9fbf130d3400.seq
     * 
     * This breaks down into the following components:
     * 
     * 1) category/data type 2) (optional) poller flow
     * 
     */
    public final static class InputFile {
        final String type, flowNumber, date, host;
        
        public static InputFile parse(String fileName) {
            String[] parts = fileName.split("_");
            String type = parts[0], flowInfo = null;
            int dot = type.indexOf('.');
            if (dot > -1) {
                String[] subInfo = type.split("\\.");
                type = subInfo[0];
                flowInfo = subInfo[1];
            }
            return new InputFile(type, flowInfo, parts[1], parts[2]);
        }
        
        private InputFile(String type, String flowNumber, String date, String host) {
            this.type = type;
            this.flowNumber = flowNumber;
            this.date = date;
            this.host = host;
        }
        
        public boolean hasFlowNumber() {
            return flowNumber != null;
        }
    }
    
    /**
     * Used to natively serialize Hadoop Counters with Gson.
     */
    public static class CountersJson implements JsonSerializer<Counters> {
        @Override
        public JsonElement serialize(Counters counters, Type type, JsonSerializationContext ctx) {
            JsonObject obj = new JsonObject();
            for (CounterGroup group : counters) {
                obj.add(group.getDisplayName(), ctx.serialize(group));
            }
            return obj;
        }
    }
    
    /**
     * Used to natively serialize Hadoop CounterGroups with Gson. CounterGroups are found within Counters objects.
     */
    public static class CounterGroupJson implements JsonSerializer<CounterGroup> {
        @Override
        public JsonElement serialize(CounterGroup cg, Type t, JsonSerializationContext ctx) {
            JsonObject obj = new JsonObject();
            for (Counter c : cg) {
                obj.addProperty(c.getDisplayName(), c.getValue());
            }
            return obj;
        }
    }
}
