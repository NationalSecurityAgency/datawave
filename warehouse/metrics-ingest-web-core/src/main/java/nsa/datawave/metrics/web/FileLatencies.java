package nsa.datawave.metrics.web;

import static com.google.common.collect.Iterators.transform;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nsa.datawave.metrics.analytic.FileLatency;
import nsa.datawave.metrics.analytic.Phase;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

@SuppressWarnings("serial")
@WebServlet
public class FileLatencies extends MetricsServlet {
    public void doGet(CloudContext connection, HttpServletRequest req, HttpServletResponse rsp) throws IOException {
        Pair<String,String> timing = super.getStartAndEnd(req);
        String start = timing.getFirst(), end = timing.getSecond();
        
        Scanner scan;
        try {
            scan = connection.createFileLatenciesScanner();
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }
        scan.setRange(new Range(start, end));
        List<FileLatency> data = Lists.newArrayList(transform(scan.iterator(), new EntryParser()));
        
        rsp.setContentType("application/json");
        new GsonBuilder().registerTypeAdapter(FileLatency.class, new FileLatencySerializer()).registerTypeAdapter(Phase.class, new PhaseSerializer()).create()
                        .toJson(data, new TypeToken<List<FileLatency>>() {}.getType(), rsp.getWriter());
        rsp.getWriter().close();
    }
}

class EntryParser implements Function<Entry<Key,Value>,FileLatency> {
    public FileLatency apply(Entry<Key,Value> e) {
        try {
            FileLatency fl = new FileLatency();
            fl.readFields(new DataInputStream(new ByteArrayInputStream(e.getValue().get())));
            return fl;
        } catch (IOException e1) {
            throw new IllegalArgumentException(e1);
        }
    }
}

class FileLatencySerializer implements JsonSerializer<FileLatency> {
    @Override
    public JsonElement serialize(FileLatency file, Type t, JsonSerializationContext ctx) {
        JsonObject j = new JsonObject();
        j.addProperty("fileName", file.getFileName());
        j.addProperty("eventCount", file.getEventCount());
        JsonArray a = new JsonArray();
        for (Phase p : file.getPhases())
            a.add(ctx.serialize(p));
        j.add("timings", a);
        return j;
    }
}

class PhaseSerializer implements JsonSerializer<Phase> {
    @Override
    public JsonElement serialize(Phase p, Type t, JsonSerializationContext ctx) {
        JsonObject obj = new JsonObject();
        obj.addProperty("name", p.name());
        obj.addProperty("start", p.start());
        obj.addProperty("end", p.end());
        return obj;
    }
}
