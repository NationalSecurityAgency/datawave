package datawave.query.attributes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;
import datawave.data.type.NoOpType;
import datawave.data.type.OneToManyNormalizerType;
import datawave.data.type.Type;
import datawave.query.collections.FunctionalSet;
import datawave.query.function.util.KryoDocumentOptions;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.webservice.query.data.ObjectSizeOf;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public class TypeAttribute<T extends Comparable<T>> extends Attribute<TypeAttribute<T>> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private static final Logger log = Logger.getLogger(TypeAttribute.class);
    
    private Type<T> datawaveType;
    
    protected TypeAttribute() {
        super(null, true);
    }
    
    public TypeAttribute(Type<T> datawaveType, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.datawaveType = datawaveType;
    }
    
    @Override
    public long sizeInBytes() {
        return ObjectSizeOf.Sizer.getObjectSize(datawaveType) + super.sizeInBytes(4);
        // 4 for datawaveType reference
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
        write(out, false);
    }
    
    @Override
    public void write(DataOutput out, boolean reducedResponse) throws IOException {
        WritableUtils.writeString(out, datawaveType.getClass().toString());
        writeMetadata(out, reducedResponse);
        WritableUtils.writeString(out, datawaveType.getDelegateAsString());
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            setDatawaveType(WritableUtils.readString(in));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
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
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.append(datawaveType.getDelegateAsString()).append(super.hashCode());
        return hcb.toHashCode();
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
        write(kryo, output, false);
    }
    
    // @Override
    public void write_(Kryo kryo, Output output, Boolean reducedResponse) {
        output.writeString(datawaveType.getClass().getName());
        super.writeMetadata(kryo, output, reducedResponse);
        output.writeString(this.datawaveType.getDelegateAsString());
        output.writeBoolean(this.toKeep);
    }
    
    // @Override
    public void writegen(Kryo kryo, Output output, Boolean reducedResponse) {
        output.writeString(datawaveType.getClass().getName());
        super.writeMetadata(kryo, output, reducedResponse);
        output.writeString(this.datawaveType.getDelegateAsString());
        output.writeBoolean(this.toKeep);
    }
    
    @Override
    public void write(Kryo kryo, Output output, Boolean reducedResponse) {
        KryoDocumentOptions documentCache = (KryoDocumentOptions) kryo.getContext().get(KryoDocumentOptions.CACHE_KEY);
        Class<? extends Type> typeClass = datawaveType.getClass();
        super.writeMetadata(kryo, output, reducedResponse);
        output.writeBoolean(toKeep);
        output.writeString(datawaveType.getDelegateAsString());
        kryo.writeClass(output, datawaveType.getClass());
        
        // Check if there is an explicit serializer for the Type
        Optional<Serializer<Type<T>>> serializer = documentCache.getSerializer(typeClass);
        boolean hasSerializer = serializer.isPresent();
        
        // Check if the type did not have an explicit serializer
        // and if so serialize the type output
        // save that we have a custom serializer attempt or not
        output.writeBoolean(hasSerializer);
        if (hasSerializer) {
            OutputChunked outputChunked = new OutputChunked(output);
            try {
                kryo.writeObject(outputChunked, datawaveType, serializer.get());
            } finally {
                outputChunked.endChunks();
                // outputChunked.flush();
            }
        }
    }
    
    // @Override
    public void read_(Kryo kryo, Input input) {
        try {
            setDatawaveType(input.readString());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            log.warn("could not read datawateType from input: " + e);
        }
        super.readMetadata(kryo, input);
        if (datawaveType == null)
            datawaveType = (Type) new NoOpType();
        
        String delegateString = input.readString();
        try {
            datawaveType.setDelegateFromString(delegateString);
        } catch (Exception ex) {
            // there was some problem with setting the delegate as the declared type.
            // Instead of letting this exception fail the query, make this a NoOpType containing the string value from the input
            log.warn("Was unable to make a " + datawaveType + " to contain a delegate created from input:" + delegateString + "  Making a NoOpType instead.");
            datawaveType = (Type) new NoOpType();
            datawaveType.setDelegateFromString(delegateString);
        }
        this.toKeep = input.readBoolean();
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        KryoDocumentOptions documentCache = (KryoDocumentOptions) kryo.getContext().get(KryoDocumentOptions.CACHE_KEY);
        
        super.readMetadata(kryo, input);
        
        toKeep = input.readBoolean();
        String delegateString = input.readString();
        Registration typeRegistration = kryo.readClass(input);
        
        if (typeRegistration == null) {
            throw new IllegalStateException("Type registration was null");
        }
        
        Class typeClass = typeRegistration.getType();
        Optional<Serializer<Type<T>>> serializer = documentCache.getSerializer(typeClass);
        boolean wasSerialized = input.readBoolean();
        try {
            if (wasSerialized) {
                InputChunked inputChunked = new InputChunked(input);
                try {
                    if (serializer.isPresent()) {
                        datawaveType = (Type<T>) kryo.readObject(inputChunked, typeClass, serializer.get());
                    }
                } catch (Exception serializeEx) {
                    log.warn("Was unable to make a " + datawaveType + " to contain a delegate created from serializer:" + delegateString
                                    + "  Will try to create from string itself.");
                } finally {
                    inputChunked.nextChunks();
                }
            }
            if (datawaveType == null) {
                datawaveType = tryReadTypeDelegateString(typeClass, delegateString);
            }
        } catch (Exception e) {
            // there was some problem with setting the delegate as the declared type.
            // Instead of letting this exception fail the query, make this a NoOpType containing the string value from the input
            log.warn("Was unable to make a " + datawaveType + " to contain a delegate created from input:" + delegateString + "  Making a NoOpType instead.");
            datawaveType = tryReadNoOpTypeDelegateString(delegateString);
        }
    }
    
    private Type<T> tryReadTypeDelegateString(String typeClassName, String delegateString) throws ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        Class<Type<T>> typeClass = (Class<Type<T>>) Class.forName(typeClassName);
        return tryReadTypeDelegateString(typeClass, delegateString);
    }
    
    private Type<T> tryReadTypeDelegateString(Class<Type<T>> typeClass, String delegateString) throws InstantiationException, IllegalAccessException {
        Type<T> typeObj = typeClass.newInstance();
        typeObj.setDelegateFromString(delegateString);
        return typeObj;
    }
    
    private Type<T> tryReadNoOpTypeDelegateString(String delegateString) {
        Type<T> typeObj = (Type<T>) new NoOpType();
        typeObj.setDelegateFromString(delegateString);
        return typeObj;
    }
    
    private void setDatawaveType(String datawaveTypeString) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        if (datawaveTypeString.startsWith("class ")) {
            datawaveTypeString = datawaveTypeString.substring("class ".length());
        }
        this.datawaveType = (Type<T>) Class.forName(datawaveTypeString).newInstance();
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
            return datawaveType.getDelegateAsString();
        } else {
            return this.getClass() + " with null delegate";
        }
    }
}
