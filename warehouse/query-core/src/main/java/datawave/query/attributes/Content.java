package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class Content extends Attribute<Content> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Type<?> normalizer = new LcNoDiacriticsType();

    private String content;
    private Attribute<?> source;

    protected Content() {
        super(null, true);
    }

    public Content(String content, Key docKey, boolean toKeep) {
        this(content, docKey, toKeep, null);
    }

    public Content(String content, Key docKey, boolean toKeep, Attribute<?> source) {
        super(docKey, toKeep);
        this.content = content;
        this.source = source;
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(content) + super.sizeInBytes(4);
        // 4 for string reference
    }

    public String getContent() {
        return this.content;
    }

    public Attribute<?> getSource() {
        return source;
    }

    @Override
    public Object getData() {
        return getContent();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, content);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
        out.writeBoolean(source != null);
        if (source != null) {
            WritableUtils.writeString(out, source.getClass().getCanonicalName());
            source.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        content = WritableUtils.readString(in);
        toKeep = WritableUtils.readVInt(in) != 0;
        boolean hasSource = in.readBoolean();
        if (hasSource) {
            String clazz = WritableUtils.readString(in);
            Class sourceClass;
            try {
                sourceClass = Class.forName(clazz);
                source = (Attribute<?>) sourceClass.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("could not parse source", e);
            }

            source.readFields(in);
        }
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
        super.writeMetadata(kryo, output);
        output.writeString(this.content);
        output.writeBoolean(this.toKeep);
        output.writeBoolean(this.source != null);
        if (source != null) {
            output.writeString(this.source.getClass().getCanonicalName());
            source.write(kryo, output);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.readMetadata(kryo, input);
        this.content = input.readString();
        this.toKeep = input.readBoolean();
        boolean hasSource = input.readBoolean();
        if (hasSource) {
            String clazz = input.readString();
            Class sourceClass;
            try {
                sourceClass = Class.forName(clazz);
                source = (Attribute<?>) sourceClass.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("could not parse source", e);
            }

            source.read(kryo, input);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Content copy() {
        return new Content(this.getContent(), this.getMetadata(), this.isToKeep(), this.getSource());
    }

}
