package datawave.query.data.parsers;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

public class ShardIndexKey implements KeyParser {

    private Key key;

    private String field;
    private String value;
    private String datatype;
    private String shard;

    @Override
    public void parse(Key k) {
        clearState();
        this.key = k;
    }

    @Override
    public void clearState() {
        this.field = null;
        this.value = null;
        this.datatype = null;
        this.shard = null;

        this.key = null;
    }

    @Override
    public String getDatatype() {
        if (datatype == null) {
            parseColumnQualifier();
        }
        return datatype;
    }

    @Override
    public String getUid() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public String getRootUid() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement this method");
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
            value = key.getRow().toString();
        }
        return value;
    }

    @Override
    public Key getKey() {
        return key;
    }

    public String getShard() {
        if (shard == null) {
            parseColumnQualifier();
        }
        return shard;
    }

    private void parseColumnQualifier() {
        ByteSequence bs = key.getColumnQualifierData();

        for (int i = 0; i < bs.length(); i++) {
            if (bs.byteAt(i) == 0x00) {
                shard = bs.subSequence(0, i).toString();
                datatype = bs.subSequence(i + 1, bs.length()).toString();
                break;
            }
        }
    }
}
