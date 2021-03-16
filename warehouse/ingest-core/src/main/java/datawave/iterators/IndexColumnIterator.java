package datawave.iterators;

import datawave.query.util.*;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.*;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class IndexColumnIterator extends TypedValueCombiner<IndexedDatesValue> {
    
    static final Logger log = Logger.getLogger(IndexColumnIterator.class);
    
    private HashMap<String,IndexedDatesValue> rowIdToCompressedIndexedDatesToCQMap = new HashMap<>();
    private String ageOffDate = "19000101";
    
    public IndexColumnIterator() {}
    
    public IndexColumnIterator(IndexColumnIterator aThis, IteratorEnvironment environment) {
        super();
        setSource(aThis.getSource().deepCopy(environment));
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        //TODO implement the function call below.
        //super.setColumns(IteratorSetting is, List< IteratorSetting.Column > columns);
        super.init(source, options, env);
        setEncoder(new IndexedDatesValueEncoder());
        if (options.get("ageOffDate") != null)
            ageOffDate = options.get("ageOffDate");
    }
    
    /**
     * Reduces a list of Values into a single Value.
     *
     * @param key
     *            The most recent version of the Key being reduced.
     *
     * @param iterator
     *            An iterator over the Values for different versions of the key.
     *
     * @return The combined Value.
     */
    @Override
    public IndexedDatesValue typedReduce(Key key, Iterator<IndexedDatesValue> iterator) {
        
        IndexedDatesValue indexedDatesValue = null;
        
        if (log.isTraceEnabled())
            log.trace("IndexColumnIterator combining dates for range for key " + key.getRow().toString(), new Exception());
        
        while (iterator.hasNext()) {
            indexedDatesValue = iterator.next();
            
            iterator.next();
        }
        
        if (indexedDatesValue == null)
            return new IndexedDatesValue();
        else
            return indexedDatesValue;
    }
    
    public static class IndexedDatesValueEncoder implements Encoder<IndexedDatesValue> {
        
        @Override
        public byte[] encode(IndexedDatesValue indexedDatesValue) {
            return indexedDatesValue.serialize().get();
        }
        
        @Override
        public IndexedDatesValue decode(byte[] bytes) throws ValueFormatException {
            
            if (bytes == null || bytes.length == 0)
                return new IndexedDatesValue();
            else
                // TODO Just deserialize with creating a Value object
                return IndexedDatesValue.deserialize(new Value(bytes));
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption("ageOffDate", "Indexed dates before this date are not aggregated.");
        io.setName("ageOffDate");
        io.setDescription("IndexColumnIterator removes entries with dates that occurred before <ageOffDate>");
        return io;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = super.validateOptions(options);
        if (valid) {
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                simpleDateFormat.parse(options.get("ageOffDate"));
            } catch (Exception e) {
                valid = false;
            }
        }
        return valid;
    }
}
