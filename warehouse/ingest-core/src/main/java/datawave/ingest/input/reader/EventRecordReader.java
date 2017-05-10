package datawave.ingest.input.reader;

import java.io.IOException;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeHelper;

import org.apache.hadoop.conf.Configuration;

/**
 * Implementers will create Event objects and validate them.
 */
public interface EventRecordReader {
    /**
     * Perform specialized event initialization as needed with the given config
     */
    public void initializeEvent(Configuration conf) throws IOException;
    
    /**
     * Return the event fully initialized and populated via the current key and value from the RecordReader.
     */
    public RawRecordContainer getEvent();
    
    /**
     * Get the raw file name
     */
    public String getRawInputFileName();
    
    /**
     * Get the raw file timestamp
     */
    public long getRawInputFileTimestamp();
    
    /**
     * Execute policy enforcement on the given event
     */
    public RawRecordContainer enforcePolicy(RawRecordContainer event);
    
    /**
     * Set the input date (???
     */
    public void setInputDate(long time);
    
    public static interface Properties extends DataTypeHelper.Properties {
        
        /**
         * Parameter to specify the name of the field in the header to use for the event date. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.date.
         */
        public static final String EVENT_DATE_FIELD_NAME = ".data.category.date";
        
        /**
         * Parameter to specify the format of the event date field in the header. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.date.format.
         */
        public static final String EVENT_DATE_FIELD_FORMAT = ".data.category.date.format";
        
        /**
         * Parameter to specify the format of the event date field in the header. This parameter supports multiple, comma-separated formats. This parameter
         * supports multiple datatypes, for example mydatatype.data.category.date.formats.
         */
        public static final String EVENT_DATE_FIELD_FORMATS = ".data.category.date.formats";
        
        /**
         * Parameter to specify the fields that should be used for the UID override. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.uid.fields.
         */
        public static final String EVENT_UID_FIELDS = ".data.category.uid.fields";
        
        /**
         * Parameter to specify the fields that contain UUID objects. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.uuid.fields.
         */
        public static final String EVENT_UUID_FIELDS = ".data.category.uuid.fields";
    }
}
