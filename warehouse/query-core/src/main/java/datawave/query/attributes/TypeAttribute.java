package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.type.DatawaveTypeIndex;
import datawave.data.type.NoOpType;
import datawave.data.type.OneToManyNormalizerType;
import datawave.data.type.Type;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.util.cache.ClassCache;
import datawave.webservice.query.data.ObjectSizeOf;

public class TypeAttribute<T extends Comparable<T>> extends Attribute<TypeAttribute<T>> implements Serializable {

    private static final long serialVersionUID = 7264249641813898860L;

    private static final Logger log = Logger.getLogger(TypeAttribute.class);

    private static final ClassCache classCache = new ClassCache();

    private Type<T> datawaveType;

    private int hashCode = Integer.MIN_VALUE;
    private String delegateString = null;

    protected TypeAttribute() {
        super(null, true);
    }

    public TypeAttribute(Type<T> datawaveType, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.datawaveType = datawaveType;
    }

    @Override
    public long sizeInBytes() {
        if (sizeInBytes == Long.MIN_VALUE) {
            // 4 for datawaveType reference
            sizeInBytes = ObjectSizeOf.Sizer.getObjectSize(datawaveType) + super.sizeInBytes(4);
        }
        return sizeInBytes;
    }

    public Type<T> getType() {
        return this.datawaveType;
    }

    @Override
    public Object getData() {
        return getType();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int index = DatawaveTypeIndex.getIndexForTypeName(datawaveType.getClass().getTypeName());
        if (index == 0) {
            // Type name not found in index, must write full name
            WritableUtils.writeVInt(out, index);
            WritableUtils.writeString(out, datawaveType.getClass().getName());
        } else {
            // Type name is present in index, do not write the full name
            WritableUtils.writeVInt(out, index);
        }

        super.writeMetadata(out);
        WritableUtils.writeString(out, datawaveType.getDelegateAsString());
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            int index = WritableUtils.readVInt(in);
            String clazzName;
            if (index == 0) {
                clazzName = WritableUtils.readString(in);
            } else {
                clazzName = DatawaveTypeIndex.getTypeNameForIndex(index);
            }
            setDatawaveType(clazzName);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException ex) {
            log.error("Could not create the datawaveType " + ex);
        }
        readMetadata(in);
        if (datawaveType == null) {
            datawaveType = (Type) new NoOpType();
        }
        this.datawaveType.setDelegateFromString(WritableUtils.readString(in));
        this.toKeep = WritableUtils.readVInt(in) != 0;
    }

    @Override
    public int compareTo(TypeAttribute<T> other) {
        int cmp = datawaveType.compareTo(other.getType());

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

        if (o instanceof TypeAttribute) {
            TypeAttribute other = (TypeAttribute) o;
            return this.getType().equals(other.getType()) && (0 == this.compareMetadata(other));
        }

        return false;
    }

    @Override
    public int hashCode() {
        if (hashcode == Integer.MIN_VALUE) {
            //  @formatter:off
            hashcode = new HashCodeBuilder(2099, 2129)
                    .append(datawaveType.getDelegateAsString())
                    .append(super.hashCode())
                    .toHashCode();
            //  @formatter:on
        }
        return hashcode;
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        if (this.datawaveType instanceof OneToManyNormalizerType) {
            Set<ValueTuple> set = new FunctionalSet<>();
            for (String norm : ((OneToManyNormalizerType<?>) this.datawaveType).getNormalizedValues()) {
                set.add(new ValueTuple(fieldNames, this.datawaveType, norm, this));
            }
            return set;
        }
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.datawaveType, datawaveType.normalize(), this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        int typeIndex = DatawaveTypeIndex.getIndexForTypeName(datawaveType.getClass().getTypeName());
        output.writeInt(typeIndex, true);
        if (typeIndex == 0) {
            // Type was not found in the TypeIndex, write the class name
            output.writeString(datawaveType.getClass().getName());
        }
        super.writeMetadata(kryo, output);
        this.datawaveType.write(kryo, output);
        output.writeBoolean(this.toKeep);
        output.writeInt(hashCode(), true);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        try {
            int typeIndex = input.readInt(true);
            String clazzName;
            if (typeIndex == 0) {
                // Type was not in the TypeIndex, must read the class name
                clazzName = input.readString();
            } else {
                // Type was in the TypeIndex, grab the name from the utility
                clazzName = DatawaveTypeIndex.getTypeNameForIndex(typeIndex);
            }
            setDatawaveType(clazzName);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            log.warn("could not read DatawaveType from input: " + e);
        }
        super.readMetadata(kryo, input);
        if (datawaveType == null) {
            datawaveType = (Type) new NoOpType();
        }
        this.datawaveType.read(kryo, input);
        this.toKeep = input.readBoolean();
        this.hashCode = input.readInt(true);
    }

    private void setDatawaveType(String datawaveTypeString)
                    throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        Class<?> clazz = classCache.get(datawaveTypeString);
        Constructor<Type> constructor = (Constructor<Type>) clazz.getDeclaredConstructor();
        this.datawaveType = constructor.newInstance();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public TypeAttribute copy() {
        return new TypeAttribute(this.getType(), this.getMetadata(), this.isToKeep());
    }

    @Override
    public String toString() {
        if (datawaveType.getDelegate() != null) {
            if (delegateString == null) {
                delegateString = datawaveType.getDelegateAsString();
            }
            return delegateString;
        } else {
            return this.getClass() + " with null delegate";
        }
    }
}
