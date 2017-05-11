package datawave.query.rewrite.jexl.functions;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

/**
 * Key adjudicator, will take an accumulo key based entry whose value is specified by T.
 * 
 * @param <T>
 */
public class KeyAdjudicator<T> implements Function<Entry<Key,T>,Entry<Key,T>> {
    
    public static final Text COLUMN_QUALIFIER_SUFFIX = new Text("\uffff");
    public static final Text EMPTY_COLUMN_QUALIFIER = new Text();
    
    private Text colQualRef = COLUMN_QUALIFIER_SUFFIX;
    
    public KeyAdjudicator(Text colQualRef) {
        this.colQualRef = colQualRef;
    }
    
    public KeyAdjudicator() {
        this.colQualRef = COLUMN_QUALIFIER_SUFFIX;
    }
    
    @Override
    public Entry<Key,T> apply(Entry<Key,T> entry) {
        final Key entryKey = entry.getKey();
        return Maps.immutableEntry(new Key(entryKey.getRow(), entryKey.getColumnFamily(), colQualRef, entryKey.getColumnVisibility(), entryKey.getTimestamp()),
                        entry.getValue());
    }
    
}
