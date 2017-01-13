package nsa.datawave.query.rewrite.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import nsa.datawave.query.jexl.DatawaveJexlContext;
import nsa.datawave.query.rewrite.collections.FunctionalSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NoOpContent extends Attribute<NoOpContent> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String content;
    
    int hashCode = 0;
    
    protected NoOpContent() {
        super(null, true);
    }
    
    public NoOpContent(String content, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setContent(content);
    }
    
    protected void setContent(String content) {
        this.content = content;
        
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.append(content).append(super.hashCode());
        
        hashCode = hcb.toHashCode();
        
    }
    
    public String getContent() {
        return this.content;
    }
    
    @Override
    public long sizeInBytes() {
        return sizeInBytes(content) + super.sizeInBytes(8);
        // 4 for string reference and int
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
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        
        content = WritableUtils.readString(in);
    }
    
    @Override
    public int compareTo(NoOpContent other) {
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
        
        if (o instanceof NoOpContent) {
            return 0 == this.compareTo((NoOpContent) o);
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return hashCode;
    }
    
    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        // NoOpNormalizer is doing this anyways...
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.content, this.content, this));
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        write(kryo, output, false);
    }
    
    @Override
    public void write(Kryo kryo, Output output, Boolean reducedResponse) {
        writeMetadata(kryo, output, reducedResponse);
        
        output.writeString(this.content);
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        
        this.content = input.readString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.attributes.Attribute#deepCopy()
     */
    @Override
    public NoOpContent copy() {
        return new NoOpContent(this.getContent(), this.getMetadata(), this.isToKeep());
    }
    
}
