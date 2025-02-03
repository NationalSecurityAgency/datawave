package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.util.StringUtils;

public class DocumentKey extends Attribute<DocumentKey> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final LcNoDiacriticsNormalizer normalizer = new LcNoDiacriticsNormalizer();

    protected DocumentKey() {
        super(null, true);
    }

    public DocumentKey(String docId, boolean toKeep) {
        super(null, toKeep);
        setId(docId);
    }

    public DocumentKey(Key docKey, boolean toKeep) {
        super(docKey, toKeep);
    }

    public DocumentKey(String shardId, String dataType, String uid, boolean toKeep) {
        super(null, toKeep);
        setKey(shardId, dataType, uid);
    }

    private void setKey(Key docKey) {
        setMetadata(docKey);
    }

    private void setId(String docId) {
        String[] parts = StringUtils.split(docId, '/');
        setKey(parts[0], parts[1], parts[2]);
    }

    private void setKey(String shardId, String dataType, String uid) {
        setMetadata(new Key(shardId, dataType + '\0' + uid, "", getColumnVisibility(), getTimestamp()));
    }

    public String getDocId() {
        StringBuilder str = new StringBuilder();
        str.append(getShardId()).append('/').append(getDataType()).append('/').append(getUid());
        return str.toString();
    }

    public Key getDocKey() {
        return getMetadata();
    }

    public String getShardId() {
        return getMetadata().getRow().toString();
    }

    public String getDataType() {
        String cf = getMetadata().getColumnFamily().toString();
        return cf.substring(0, cf.indexOf('\0'));
    }

    public String getUid() {
        String cf = getMetadata().getColumnFamily().toString();
        return cf.substring(cf.indexOf('\0') + 1);
    }

    @Override
    public Object getData() {
        return getDocId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, getShardId());
        WritableUtils.writeString(out, getDataType());
        WritableUtils.writeString(out, getUid());
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        String shardId = WritableUtils.readString(in);
        String dataType = WritableUtils.readString(in);
        String uid = WritableUtils.readString(in);
        setKey(shardId, dataType, uid);
        this.toKeep = WritableUtils.readVInt(in) != 0;
    }

    @Override
    public int compareTo(DocumentKey other) {
        // Compare the ColumnVisibility as well
        return this.compareMetadata(other);
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof DocumentKey) {
            return 0 == this.compareTo((DocumentKey) o);
        }

        return false;
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.getDocId(), normalizer.normalize(this.getDocId()), this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.writeMetadata(kryo, output);
        output.writeString(this.getShardId());
        output.writeString(this.getDataType());
        output.writeString(this.getUid());
        output.writeBoolean(this.isToKeep());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.readMetadata(kryo, input);
        String shardId = input.readString();
        String dataType = input.readString();
        String uid = input.readString();
        setKey(shardId, dataType, uid);
        setToKeep(input.readBoolean());
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public DocumentKey copy() {
        return new DocumentKey(this.getMetadata(), this.isToKeep());
    }

}
