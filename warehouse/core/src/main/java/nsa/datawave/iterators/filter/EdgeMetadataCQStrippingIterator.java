package nsa.datawave.iterators.filter;

import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import nsa.datawave.edge.util.EdgeKey;
import nsa.datawave.util.StringUtils;

/**
 * Removes the optional attributes, "attribute2" and "attribute3" fields, from the edge entries in the metadata table. Should be used only at scan time in
 * conjunction with the EdgeMetadataCombiner.
 */
public class EdgeMetadataCQStrippingIterator extends WrappingIterator {
    
    @Override
    public Key getTopKey() {
        Key key = super.getTopKey();
        if (key.getColumnFamily().equals(nsa.datawave.data.ColumnFamilyConstants.COLF_EDGE)) {
            return transformKey(key);
        } else {
            return key;
        }
        
    }
    
    public static Key transformKey(Key key) {
        String[] pieces = StringUtils.split(key.getColumnQualifier().toString(), EdgeKey.COL_SEPARATOR);
        Text cq = new Text(pieces[0]);
        // notice that the visibility is being thrown away here now. It is not needed unless
        // optional attributes 2 and 3 are present.
        return new Key(key.getRow(), key.getColumnFamily(), cq, new Text(), key.getTimestamp());
    }
}
