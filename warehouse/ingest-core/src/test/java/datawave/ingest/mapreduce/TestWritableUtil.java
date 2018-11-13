package datawave.ingest.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utilties for converting between Writables and non-writables.
 */
public class TestWritableUtil {
    
    /**
     * Converts a Map of Strings into a Writable and writes it.
     *
     * @param map
     * @param output
     * @throws IOException
     */
    public static void writeMap(Map<String,String> map, DataOutput output) throws IOException {
        MapWritable mw = new MapWritable();
        
        for (Map.Entry<String,String> entry : map.entrySet()) {
            mw.put(new Text(entry.getKey()), new Text(entry.getValue()));
        }
        
        mw.write(output);
    }
    
    /**
     * Reads a map of Strings from a DataInput.
     *
     * @param input
     * @return map of strings
     * @throws IOException
     */
    public static Map<String,String> readMap(DataInput input) throws IOException {
        MapWritable mw = new MapWritable();
        mw.readFields(input);
        
        Map<String,String> map = new TreeMap<>();
        
        for (Map.Entry<Writable,Writable> entry : mw.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        
        return map;
    }
    
    /**
     * Converts a collection of strings into an ArrayWritable and writes it to the given output.
     *
     * @param coll
     * @param output
     * @throws IOException
     */
    public static void writeCollection(Collection<String> coll, DataOutput output) throws IOException {
        ArrayWritable aw = new ArrayWritable(Text.class);
        Writable[] writables = new Writable[coll.size()];
        
        Iterator<String> iter = coll.iterator();
        for (int i = 0; i < writables.length; ++i) {
            writables[i] = new Text(iter.next());
        }
        
        aw.set(writables);
        aw.write(output);
    }
    
    /**
     * Reads a collection of Strings back from a DataInput.
     *
     * @param input
     * @return collection of strings
     * @throws IOException
     */
    public static Collection<String> readCollection(DataInput input) throws IOException {
        ArrayWritable aw = new ArrayWritable(Text.class);
        aw.readFields(input);
        
        Collection<String> coll = new LinkedList<>();
        Writable[] arr = aw.get();
        
        for (int i = 0; i < arr.length; ++i) {
            coll.add(arr[i].toString());
        }
        
        return coll;
    }
}
