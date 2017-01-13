package nsa.datawave.ingest.data;

import nsa.datawave.data.hash.UID;

import org.apache.accumulo.core.security.ColumnVisibility;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * Generic container to hold a "RawRecord" which will be used in various parts of the ingest framework.
 */
public interface RawRecordContainer {
    
    Map<String,String> getSecurityMarkings();
    
    void setSecurityMarkings(Map<String,String> securityMarkings);
    
    void addSecurityMarking(String domain, String marking);
    
    boolean hasSecurityMarking(String domain, String marking);
    
    UID getId();
    
    void setId(UID id);
    
    Type getDataType();
    
    void setDataType(Type dataType);
    
    long getDate();
    
    void setDate(long date);
    
    Collection<String> getErrors();
    
    void setErrors(Collection<String> errors);
    
    void addError(String error);
    
    void removeError(String error);
    
    boolean hasError(String error);
    
    boolean fatalError();
    
    boolean ignorableError();
    
    void clearErrors();
    
    Collection<String> getAltIds();
    
    void setAltIds(Collection<String> altIds);
    
    void addAltId(String altId);
    
    boolean hasAltId(String altId);
    
    String getRawFileName();
    
    void setRawFileName(String rawFileName);
    
    long getRawRecordNumber();
    
    void setRawRecordNumber(long rawRecordNumber);
    
    long getRawFileTimestamp();
    
    void setRawFileTimestamp(long rawRecordTimestamp);
    
    byte[] getRawData();
    
    void setRawData(byte[] rawData);
    
    Object getAuxData();
    
    void setAuxData(Object auxData);
    
    void setRawDataAndGenerateId(byte[] rawData);
    
    void setRawDataAndGenerateId(byte[] rawData, String extra);
    
    RawRecordContainer copy();
    
    long getDataOutputSize();
    
    void write(DataOutput dataOutput) throws IOException;
    
    ColumnVisibility getVisibility();
    
    void setVisibility(ColumnVisibility visibility);
    
    /**
     * Return a date to use in the UID for this RawRecord
     * 
     * @return null or date object
     */
    Date getTimeForUID();
    
    /**
     * Does this record need to be masked.
     * 
     * @return true or false
     */
    boolean isRequiresMasking();
    
    /**
     * Clear/reset the current state to support object reuse
     */
    void clear();
}
