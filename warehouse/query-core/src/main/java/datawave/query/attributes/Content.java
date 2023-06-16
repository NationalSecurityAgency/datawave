package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.collections.FunctionalSet;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Content extends Attribute<Content> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Type<?> normalizer = new LcNoDiacriticsType();

    private String content;

    protected Content() {
        super(null, true);
    }

    public Content(String content, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.content = content;
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(content) + super.sizeInBytes(4);
        // 4 for string reference
    }

    public String getContent() {
        return this.content;
    }

    @Override
    public Object getData() {
        return getContent();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        write(out, false);
    }

    @Override
    public void write(DataOutput out, boolean reducedResponse) throws IOException {
        writeMetadata(out, reducedResponse);
        WritableUtils.writeString(out, content);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        content = WritableUtils.readString(in);
        toKeep = WritableUtils.readVInt(in) != 0;
    }

    @Override
    public int compareTo(Content other) {
        int cmp = content.compareTo(other.getContent());

        if (0 == cmp) {
            // Compare the ColumnVisibility as well
            return this.compareMetadata(other);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof Content) {
            return 0 == this.compareTo((Content) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.append(content).append(super.hashCode());

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.content, normalizer.normalize(this.content), this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        write(kryo, output, false);
    }

    @Override
    public void write(Kryo kryo, Output output, Boolean reducedResponse) {
        super.writeMetadata(kryo, output, reducedResponse);
        output.writeString(this.content);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.readMetadata(kryo, input);
        this.content = input.readString();
        this.toKeep = input.readBoolean();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Content copy() {
        return new Content(this.getContent(), this.getMetadata(), this.isToKeep());
    }

}
