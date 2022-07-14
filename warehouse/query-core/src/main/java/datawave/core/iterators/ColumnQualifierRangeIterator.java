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
 * This class enables range queries based on Column Qualifiers
 * 
 */
public class ColumnQualifierRangeIterator extends ColumnRangeIterator {
    public ColumnQualifierRangeIterator() {
        super();
    }
    
    public ColumnQualifierRangeIterator(SortedKeyValueIterator<Key,Value> source) {
        super(source);
    }
    
    public ColumnQualifierRangeIterator(SortedKeyValueIterator<Key,Value> source, Range columnFamilyRange) {
        super(source, columnFamilyRange);
    }
    
    @Override
    protected void consumeImpl() throws IOException {
        
        int count = 0;
        int limit = getSkipLimit();
        
        while (getSource().hasTop()) {
            Key topColumnQual = new Key(getSource().getTopKey().getColumnQualifier());
            
            if (getColumnRange().beforeStartKey(topColumnQual)) { // top key's CQ is before the desired range starts, need to skip some CQs...
                
                if (count < limit) {
                    advanceSource();
                    ++count;
                } else {
                    Text row = getSource().getTopKey().getRow();
                    Text cf = getSource().getTopKey().getColumnFamily();
                    Text startColQual = getColumnStart();
                    
                    Key nextKey = new Key(row, cf, startColQual);
                    reseek(nextKey);
                    count = 0;
                }
            } else if (getColumnRange().afterEndKey(topColumnQual)) { // reached the end of the desired CQ range, need to go to the next CF or row
                if (count < limit) {
                    advanceSource();
                    ++count;
                } else {
                    Text row = getSource().getTopKey().getRow();
                    Text nextCF = new Text(followingArray(getSource().getTopKey().getColumnFamily().getBytes()));
                    Text startColQual = getColumnStart();
                    
                    Key nextKey = new Key(row, nextCF, startColQual);
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
        return new ColumnQualifierRangeIterator(getSource().deepCopy(env), getColumnRange());
    }
    
}
