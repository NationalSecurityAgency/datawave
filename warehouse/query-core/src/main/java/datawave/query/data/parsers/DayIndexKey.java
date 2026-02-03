package datawave.query.data.parsers;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * Day Index key structure
 *
 * <pre>
 *     row: shard-null-value
 *     columnFamily: field
 *     columnQualifier: datatype
 *     value: bitset
 * </pre>
 */
public class DayIndexKey implements KeyParser {

    private Key key;

    private ByteSequence row;

    private String shard;
    private String field;
    private String value;
    private String datatype;

    @Override
    public void parse(Key k) {
        clearState();
        this.key = k;
    }

    @Override
    public void clearState() {
        row = null;

        shard = null;
        field = null;
        value = null;
        datatype = null;
    }

    @Override
    public String getDatatype() {
        if (datatype == null) {
            datatype = key.getColumnQualifier().toString();
        }
        return datatype;
    }

    @Override
    public String getUid() {
        return null;
    }

    @Override
    public String getRootUid() {
        return null;
    }

    @Override
    public String getField() {
        if (field == null) {
            field = key.getColumnFamily().toString();
        }
        return field;
    }

    @Override
    public String getValue() {
        if (value == null) {
            scanRow();
        }
        return value;
    }

    public String getShard() {
        if (shard == null) {
            scanRow();
        }
        return shard;
    }

    @Override
    public Key getKey() {
        return key;
    }

    private void scanRow() {
        if (key == null) {
            return;
        }

        row = key.getRowData();

        for (int i = 0; i < row.length(); i++) {
            if (row.byteAt(i) == 0x00) {
                shard = row.subSequence(0, i).toString();
                value = row.subSequence(i + 1, row.length()).toString();
                break;
            }
        }
    }
}
