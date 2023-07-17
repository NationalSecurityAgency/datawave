package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import datawave.query.tld.TLD;

/**
 * For use in the {@link datawave.query.ancestor.AncestorQueryIterator}
 * <p>
 *
 */
public class AncestorRangeProvider extends DocumentRangeProvider {

    /**
     * The start key is remapped to the beginning of the TLD to get all necessary fields
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
