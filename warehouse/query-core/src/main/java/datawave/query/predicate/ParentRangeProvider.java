package datawave.query.predicate;

import datawave.query.function.DocumentRangeProvider;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

/**
 * For use with the {@link datawave.query.tables.ParentQueryLogic}
 */
public class ParentRangeProvider extends DocumentRangeProvider {
    
    /**
     * Get the start key by parsing the root uid from the column family
     * 
     * @param k
     *            an initial key
     * @return the start key
     */
    @Override
    public Key getStartKey(Key k) {
        Text cf = new Text(TLD.parseParentPointerFromId(k.getColumnFamilyData()).toArray());
        return new Key(k.getRow(), cf);
    }
}
