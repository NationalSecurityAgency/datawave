package datawave.query.data.parsers;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

public class ShardIndexKey implements KeyParser {

    private Key key;

    private ByteSequence cq;
    private int cqSplit;

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
        this.cq = null;
        this.cqSplit = -1;

        this.field = null;
        this.value = null;
        this.datatype = null;
        this.shard = null;
    }

    @Override
    public String getDatatype() {
        if (datatype == null) {
            if (cq == null) {
                cq = key.getColumnQualifierData();
                for (int i = cq.length() - 1; i > 0; i--) {
                    if (cq.byteAt(i) == 0x00) {
                        cqSplit = i;
                        break;
                    }
                }
            }
            datatype = cq.subSequence(cqSplit + 1, cq.length()).toString();
        }
        return datatype;
    }

    public String getShard() {
        if (shard == null) {
            if (cq == null) {
                cq = key.getColumnQualifierData();
                for (int i = 0; i < cq.length(); i++) {
                    if (cq.byteAt(i) == 0x00) {
                        cqSplit = i;
                        break;
                    }
                }
            }
            shard = cq.subSequence(0, cqSplit).toString();
        }
        return shard;
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
}
