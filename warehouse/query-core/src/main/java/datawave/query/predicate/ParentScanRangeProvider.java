package datawave.query.predicate;

import datawave.query.function.DocumentScanRangeProvider;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

/**
 * For use with the {@link datawave.query.tables.AncestorQueryLogic}
 */
public class ParentScanRangeProvider extends DocumentScanRangeProvider {
    
    /**
     * Get the start key by parsing the root uid from the column family
     * 
     * @param k
     *            an initial key
     * @return the start key
     */
    @Override
    public Key getStartKey(Key k) {
        Text cf = new Text(TLD.parseRootPointerFromId(k.getColumnFamily().toString()));
        return new Key(k.getRow(), cf);
    }
}
