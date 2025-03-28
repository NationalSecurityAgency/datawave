package datawave.query.util;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;

import datawave.data.ColumnFamilyConstants;

/**
 * A unique entry in the DatawaveMetadata is defined by the combination of fieldname and datatype
 */
public class MetadataEntry {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    
    private static final String FIELDNAME = "FieldName: ", DATATYPE = "Datatype: ";
    
    private String fieldName, datatype;
    
    public MetadataEntry(Key k) {
        if (null == k) {
            throw new IllegalArgumentException("Key is null");
        }
        
        if (!ColumnFamilyConstants.COLF_DESC.equals(k.getColumnFamily())) {
            throw new IllegalArgumentException("Key was not a 'description'.");
        }
        
        setFieldName(k.getRow().toString());
        setDatatype(k.getColumnQualifier().toString());
    }
    
    public MetadataEntry(Text fieldName, Text datatype) {
        this(fieldName.toString(), datatype.toString());
    }
    
    public MetadataEntry(String fieldName, String datatype) {
        setFieldName(fieldName);
        setDatatype(datatype);
    }
    
    protected void setFieldName(String fieldName) {
        if (null == fieldName) {
            throw new IllegalArgumentException("FieldName is null");
        }
        
        this.fieldName = fieldName.toUpperCase();
    }
    
    protected void setDatatype(String datatype) {
        if (null == datatype) {
            throw new IllegalArgumentException("Datatype is null");
        }
        
        this.datatype = datatype.toLowerCase();
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public String getDatatype() {
        return datatype;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof MetadataEntry) {
            MetadataEntry other = (MetadataEntry) o;
            
            // Asserted to be non-null by constructor
            return this.fieldName.equals(other.getFieldName()) && this.datatype.equals(other.getDatatype());
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return this.fieldName.hashCode() ^ this.datatype.hashCode();
    }
    
    @Override
    public String toString() {
        // Allocate enough up front
        StringBuilder sb = new StringBuilder(FIELDNAME.length() + this.fieldName.length() + DATATYPE.length() + this.datatype.length() + 2);
        sb.append(FIELDNAME).append(this.fieldName);
        sb.append(", ");
        sb.append(DATATYPE).append(this.datatype);
        
        return sb.toString();
    }
    
    public Entry<String,String> toEntry() {
        return Maps.immutableEntry(this.fieldName, this.datatype);
    }
    
}
