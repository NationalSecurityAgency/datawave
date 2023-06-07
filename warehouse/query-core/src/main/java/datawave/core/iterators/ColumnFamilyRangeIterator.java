package datawave.core.iterators;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * 
 * This class enables range queries based on Column Families
 * 
 * 
 * 
 */
public class ColumnFamilyRangeIterator extends ColumnRangeIterator {
    public ColumnFamilyRangeIterator() {
        super();
    }
    
    public ColumnFamilyRangeIterator(SortedKeyValueIterator<Key,Value> source) {
        super(source);
    }
    
    public ColumnFamilyRangeIterator(SortedKeyValueIterator<Key,Value> source, Range columnFamilyRange) {
        super(source, columnFamilyRange);
    }
    
    @Override
    protected void consumeImpl() throws IOException {
        
        int count = 0;
        int limit = getSkipLimit();
        
        while (getSource().hasTop()) {
            Key topColumnFamily = new Key(getSource().getTopKey().getColumnFamily());
            
            if (getColumnRange().beforeStartKey(topColumnFamily)) { // top key's CF is before the desired range starts, need to skip some CFs...
                
                if (count < limit) {
                    advanceSource();
                    ++count;
                } else {
                    Text row = getSource().getTopKey().getRow();
                    Text startColFam = getColumnStart();
                    Key nextKey = new Key(row, startColFam);
                    reseek(nextKey);
                    count = 0;
                }
            } else if (getColumnRange().afterEndKey(topColumnFamily)) { // reached the end of the desired CF range, need to go to the next row
                if (count < limit) {
                    advanceSource();
                    ++count;
                } else {
                    Text nextRow = new Text(followingArray(getSource().getTopKey().getRow().getBytes()));
                    Text startColFam = getColumnStart();
                    Key nextKey = new Key(nextRow, startColFam);
                    reseek(nextKey);
                    count = 0;
                }
            } else { // within the range, break the consuming loop...
                break;
            }
        } // end while()
    } // end consume()
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ColumnFamilyRangeIterator(getSource().deepCopy(env), getColumnRange());
    }
}
