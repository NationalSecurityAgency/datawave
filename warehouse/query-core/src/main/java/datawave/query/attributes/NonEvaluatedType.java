package datawave.query.attributes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.NoOpType;
import datawave.data.type.OneToManyNormalizerType;
import datawave.data.type.Type;
import datawave.query.collections.FunctionalSet;
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
import java.util.Set;

public class NonEvaluatedType<T extends Comparable<T>> extends Attribute<NonEvaluatedType<T>> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger log = Logger.getLogger(NonEvaluatedType.class);

    private Type<T> datawaveType;
    private String originalData;
    private Class<Type<T>> typeClass;

    protected NonEvaluatedType() {
        super(null, true);
    }

    public NonEvaluatedType(Type<T> datawaveType, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.datawaveType = datawaveType;
    }
    
    @Override
    public long sizeInBytes() {
        return ObjectSizeOf.Sizer.getObjectSize(originalData) + super.sizeInBytes(4);
        // 4 for datawaveType reference
    }
    
    public Type<T> getType() {
        createIfNeeded();
        return this.datawaveType;
    }
    
    @Override
    public Object getData() {
        return getType();
    }

    private void createIfNeeded(){

    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        write(out, false);
    }
    
    @Override
    public void write(DataOutput out, boolean reducedResponse) throws IOException {
        WritableUtils.writeString(out, typeClass.toString());
        writeMetadata(out, reducedResponse);
        WritableUtils.writeString(out, originalData);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            setDatawaveType(WritableUtils.readString(in));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            log.error("Could not create the datawaveType " + ex);
        }
        readMetadata(in);
        if (datawaveType == null)
            datawaveType = (Type) new NoOpType();
        datawaveType.setDelegateFromString(WritableUtils.readString(in));
    }

    public String getOriginalData(){
        return originalData;
    }
    
    @Override
    public int compareTo(NonEvaluatedType<T> other) {
        int cmp = originalData.compareTo(other.getOriginalData());
        
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
        
        if (o instanceof NonEvaluatedType) {
            NonEvaluatedType other = (NonEvaluatedType) o;
            return this.getType().equals(other.getType()) && (0 == this.compareMetadata(other));
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.append(originalData).append(super.hashCode());
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
    
    @Override
    public void write(Kryo kryo, Output output, Boolean reducedResponse) {
        output.writeString(typeClass.getName());
        super.writeMetadata(kryo, output, reducedResponse);
        
        output.writeString(this.originalData);
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
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
    }
    
    private void setDatawaveType(String datawaveTypeString) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        this.typeClass = (Class<Type<T>>) Class.forName(datawaveTypeString);
        this.datawaveType = (Type<T>) typeClass.newInstance();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see Attribute#deepCopy()
     */
    @Override
    public NonEvaluatedType copy() {
        return new NonEvaluatedType(this.getType(), this.getMetadata(), this.isToKeep());
    }
    
    @Override
    public String toString() {
        if (originalData != null) {
            return originalData;
        } else {
            return this.getClass() + " with null delegate";
        }
    }
}
