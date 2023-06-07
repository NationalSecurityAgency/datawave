package datawave.metrics.iterators;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Iterator that assumes the returned value is a {@link Counters} object and returns that value as a Json string.
 */
public class JsonCountersIterator extends WrappingIterator implements OptionDescriber {
    private static String PRETTY_PRINT_OPT = "prettyPrint";
    private static Logger log = Logger.getLogger(JsonCountersIterator.class);
    private boolean prettyPrint = false;
    private Gson gson;
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.containsKey(PRETTY_PRINT_OPT)) {
            prettyPrint = Boolean.parseBoolean(options.get(PRETTY_PRINT_OPT));
        }
        gson = initializeGson();
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        JsonCountersIterator copy;
        try {
            copy = getClass().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            // Shouldn't happen so just throw as a runtime exception
            throw new RuntimeException(e);
        }
        
        copy.setSource(getSource().deepCopy(env));
        copy.prettyPrint = prettyPrint;
        copy.gson = copy.initializeGson();
        return copy;
    }
    
    private Gson initializeGson() {
        GsonBuilder builder = new GsonBuilder().registerTypeAdapter(Counters.class, new CountersJson()).registerTypeAdapter(CounterGroup.class,
                        new CounterGroupJson());
        if (prettyPrint)
            builder.setPrettyPrinting();
        return builder.create();
    }
    
    @Override
    public Value getTopValue() {
        Value topValue = super.getTopValue();
        if (topValue == null) {
            return null;
        } else {
            try {
                Counters counters = new Counters();
                counters.readFields(ByteStreams.newDataInput(topValue.get()));
                String json = gson.toJson(counters);
                return new Value(json.getBytes());
            } catch (IOException e) {
                log.debug("Unable to parse value for key " + getTopKey() + " as a counters", e);
                return topValue;
            }
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions opts = new IteratorOptions("jsonCounters",
                        getClass().getSimpleName() + " returns values that are Hadoop Counters objects serialized in Json form", null, null);
        opts.addNamedOption(PRETTY_PRINT_OPT, "Indicates whether or not the json output should be formatted for human readability (default is false)");
        return opts;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = (options == null || options.isEmpty());
        if (!valid && options.containsKey(PRETTY_PRINT_OPT)) {
            try {
                Boolean.parseBoolean(options.get(PRETTY_PRINT_OPT));
                valid = true;
            } catch (Exception e) {
                // ignore -- it's an invalid option
            }
        }
        return valid;
    }
    
    /**
     * Serializes a Hadoop {@link Counters} object to Json using Gson. Each {@link CounterGroup} is placed in the Json object using the group name (see
     * {@link CounterGroup#getName}) as the property name.
     */
    public static class CountersJson implements JsonSerializer<Counters> {
        @Override
        public JsonElement serialize(org.apache.hadoop.mapreduce.Counters counters, Type type, JsonSerializationContext ctx) {
            JsonObject obj = new JsonObject();
            for (CounterGroup group : counters) {
                obj.add(group.getName(), ctx.serialize(group));
            }
            return obj;
        }
    }
    
    /**
     * Serializes a Hadoop {@link CounterGroup} using to Json using Gson. The group includes a displayName property if the display name differs from the name.
     * Then each {@link Counter} object is serialized as a child using the name (see {@link Counter#getName()}) as the key. If any of the display names of the
     * counters in ths group differs from the name, then a displayNames object is included, where each property of the object maps the counter name to its
     * display name.
     */
    public static class CounterGroupJson implements JsonSerializer<CounterGroup> {
        @Override
        public JsonElement serialize(CounterGroup cg, Type t, JsonSerializationContext ctx) {
            JsonObject obj = new JsonObject();
            if (!cg.getName().equals(cg.getDisplayName()))
                obj.addProperty("displayName", cg.getDisplayName());
            JsonObject dns = new JsonObject();
            boolean anyNamesDiffer = false;
            for (Counter c : cg) {
                obj.addProperty(c.getName(), c.getValue());
                if (!c.getName().equals(c.getDisplayName()))
                    anyNamesDiffer = true;
                dns.addProperty(c.getName(), c.getDisplayName());
            }
            if (anyNamesDiffer)
                obj.add("displayNames", dns);
            return obj;
        }
    }
}
