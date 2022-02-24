package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.FilterIngest;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.time.Now;
import datawave.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

import java.util.Date;
import java.util.Map;

public class FieldHarvester {
    private static final Logger log = Logger.getLogger(FieldHarvester.class);
    
    private static Now now = Now.getInstance();
    
    public static final String LOAD_DATE_FIELDNAME = "LOAD_DATE";
    public static final String SEQUENCE_FILE_FIELDNAME = "ORIG_FILE";
    public static final String LOAD_SEQUENCE_FILE_NAME = "ingest.event.mapper.load.seq.filename";
    public static final String TRIM_SEQUENCE_FILE_NAME = "ingest.event.mapper.trim.sequence.filename";
    public static final String RAW_FILE_FIELDNAME = "RAW_FILE";
    public static final String LOAD_RAW_FILE_NAME = "ingest.event.mapper.load.raw.filename";
    
    private boolean createSequenceFileName;
    private boolean trimSequenceFileName;
    private boolean createRawFileName;
    private final DateNormalizer dateNormalizer = new DateNormalizer();
    
    private static final String SRC_FILE_DEL = "|";
    private Exception exception;
    
    public FieldHarvester(Configuration configuration) {
        this.createSequenceFileName = configuration.getBoolean(LOAD_SEQUENCE_FILE_NAME, true);
        this.trimSequenceFileName = configuration.getBoolean(TRIM_SEQUENCE_FILE_NAME, true);
        this.createRawFileName = configuration.getBoolean(LOAD_RAW_FILE_NAME, true);
    }
    
    public boolean hasError() {
        return null != this.exception;
    }
    
    public Exception getException() {
        return this.exception;
    }
    
    /**
     * Updates "fields" with extracted, derived, and automatically generated fields.
     *
     * @param fields
     *            the Multimap to modify with extracted and generated fields
     * @param ingestHelper
     *            interface to use for field extraction
     * @param value
     *            the record from which the fields will be extracted
     * @param offset
     *            record offset within the source file
     * @param splitStart
     *            the splitStart for the record
     */
    public void extractFields(Multimap<String,NormalizedContentInterface> fields, IngestHelperInterface ingestHelper, RawRecordContainer value, long offset,
                    String splitStart) {
        // reset exception-in-extraction tracking
        this.exception = null;
        
        // "candidateFields" holds the fields that may eventually be added to "fields"
        Multimap<String,NormalizedContentInterface> candidateFields = null;
        try {
            // get salvaged fields if getEventFields throws exception
            candidateFields = faultTolerantGetEventFields(value, ingestHelper);
            
            // try adding supplemental fields to candidateFields, whether or not there was an exception
            addSupplementalFields(value, offset, splitStart, ingestHelper, candidateFields);
        } catch (Exception exception) {
            this.exception = exception;
        } finally {
            // Add each "candidateFields" field value to "fields" as long as the field value is without error
            addErrorFreeFields(fields, candidateFields);
        }
    }
    
    /**
     * Calls IngestHelper.getEventFields with value. If an exception is thrown, captures it and attempts to salvage fields from the value.
     */
    // package method visibility for EventMapper.getFields only
    @Deprecated
    Multimap<String,NormalizedContentInterface> faultTolerantGetEventFields(RawRecordContainer value, IngestHelperInterface ingestHelper) {
        try {
            // Parse the event into its candidate field names and values using the IngestHelperInterface.
            return ingestHelper.getEventFields(value);
        } catch (Exception exception) {
            // delay throwing the exception
            this.exception = exception;
            return attemptToSalvageFields(value, ingestHelper);
        }
    }
    
    // todo test case where salvage fields are empty
    // package method visibility for EventMapper.getFields only
    void addSupplementalFields(RawRecordContainer value, long offset, String splitStart, IngestHelperInterface ingestHelper,
                    Multimap<String,NormalizedContentInterface> fields) {
        addVirtualFields(ingestHelper, fields);
        addCompositeFields(ingestHelper, fields);
        addLoadDateField(fields);
        addFileNameFields(value, offset, splitStart, fields);
        applyFieldFilters(ingestHelper, fields);
    }
    
    /*
     * Populate the "fields" method parameter with any candidateFields that do not have an Error
     */
    private void addErrorFreeFields(Multimap<String,NormalizedContentInterface> fields, Multimap<String,NormalizedContentInterface> candidateFields) {
        if (null == candidateFields) {
            return;
        }
        Throwable fieldError = null;
        for (Map.Entry<String,NormalizedContentInterface> entry : candidateFields.entries()) {
            // noinspection ThrowableResultOfMethodCallIgnored
            if (entry.getValue().getError() != null) {
                fieldError = entry.getValue().getError();
            }
            fields.put(entry.getKey(), entry.getValue());
        }
        if (fieldError != null) {
            this.exception = new FieldNormalizationError("Failed getting all fields", fieldError);
        }
    }
    
    private void addVirtualFields(IngestHelperInterface ingestHelper, Multimap<String,NormalizedContentInterface> newFields) {
        // Also get the virtual fields, if applicable.
        if (ingestHelper instanceof VirtualIngest) {
            VirtualIngest vHelper = (VirtualIngest) ingestHelper;
            Multimap<String,NormalizedContentInterface> virtualFields = vHelper.getVirtualFields(newFields);
            for (Map.Entry<String,NormalizedContentInterface> v : virtualFields.entries())
                newFields.put(v.getKey(), v.getValue());
        }
    }
    
    private void addCompositeFields(IngestHelperInterface ingestHelper, Multimap<String,NormalizedContentInterface> newFields) {
        // Also get the composite fields, if applicable
        if (ingestHelper instanceof CompositeIngest) {
            CompositeIngest vHelper = (CompositeIngest) ingestHelper;
            Multimap<String,NormalizedContentInterface> compositeFields = vHelper.getCompositeFields(newFields);
            for (String fieldName : compositeFields.keySet()) {
                // if this is an overloaded composite field, we are replacing the existing field data
                if (vHelper.isOverloadedCompositeField(fieldName)) {
                    newFields.removeAll(fieldName);
                }
                newFields.putAll(fieldName, compositeFields.get(fieldName));
            }
        }
    }
    
    private void addLoadDateField(Multimap<String,NormalizedContentInterface> newFields) {
        // Create a LOAD_DATE parameter, which is the current time in milliseconds, for all datatypes
        long loadDate = now.get();
        NormalizedFieldAndValue loadDateValue = new NormalizedFieldAndValue(LOAD_DATE_FIELDNAME, Long.toString(loadDate));
        // set an indexed field value for use by the date index data type handler
        loadDateValue.setIndexedFieldValue(dateNormalizer.normalizeDelegateType(new Date(loadDate)));
        newFields.put(LOAD_DATE_FIELDNAME, loadDateValue);
    }
    
    private void addRawFileField(RawRecordContainer value, Multimap<String,NormalizedContentInterface> newFields, String seqFileName) {
        if (createRawFileName && !value.getRawFileName().isEmpty() && !value.getRawFileName().equals(seqFileName)) {
            newFields.put(RAW_FILE_FIELDNAME, new NormalizedFieldAndValue(RAW_FILE_FIELDNAME, value.getRawFileName()));
        }
    }
    
    private void addOrigFileField(Multimap<String,NormalizedContentInterface> newFields, long offset, String splitStart, String seqFileName) {
        if (null != seqFileName) {
            StringBuilder seqFile = new StringBuilder(seqFileName);
            
            seqFile.append(SRC_FILE_DEL).append(offset);
            
            if (null != splitStart) {
                seqFile.append(SRC_FILE_DEL).append(splitStart);
            }
            
            newFields.put(SEQUENCE_FILE_FIELDNAME, new NormalizedFieldAndValue(SEQUENCE_FILE_FIELDNAME, seqFile.toString()));
        }
    }
    
    private String getSeqFileName() {
        String seqFileName;
        seqFileName = NDC.peek();
        
        if (trimSequenceFileName) {
            seqFileName = StringUtils.substringAfterLast(seqFileName, "/");
        }
        return seqFileName;
    }
    
    private void addFileNameFields(RawRecordContainer value, long offset, String splitStart, Multimap<String,NormalizedContentInterface> newFields) {
        String seqFileName = null;
        
        if (createSequenceFileName) {
            seqFileName = getSeqFileName();
            
            // place the sequence filename into the event
            addOrigFileField(newFields, offset, splitStart, seqFileName);
        }
        
        addRawFileField(value, newFields, seqFileName);
    }
    
    private void applyFieldFilters(IngestHelperInterface ingestHelper, Multimap<String,NormalizedContentInterface> newFields) {
        // Also if this helper needs to filter the fields before returning, apply now
        if (ingestHelper instanceof FilterIngest) {
            FilterIngest fHelper = (FilterIngest) ingestHelper;
            fHelper.filter(newFields);
        }
    }
    
    /**
     * If IngestHelper implements FieldSalvager, get the salvageable fields from value. Otherwise, return an empty Multimap.
     */
    private Multimap<String,NormalizedContentInterface> attemptToSalvageFields(RawRecordContainer value, IngestHelperInterface ingestHelper) {
        // If this helper is able, attempt to salvage a subset of the fields
        if (null != ingestHelper && ingestHelper instanceof FieldSalvager) {
            FieldSalvager salvager = (FieldSalvager) ingestHelper;
            try {
                Multimap<String,NormalizedContentInterface> salvagedFields = salvager.getSalvageableEventFields(value);
                if (null != salvagedFields) {
                    return salvagedFields;
                }
            } catch (Exception salvagerException) {
                // Do not overwrite the original exception
                if (null == this.exception) {
                    this.exception = new IllegalStateException("Unexpected state (FieldExpander.exception should be non-null if salvaging", salvagerException);
                } else {
                    // allow original exception (this.exception) to be thrown by caller
                    log.error("Even salvager threw an exception", salvagerException);
                }
            }
        }
        return HashMultimap.create();
    }
    
    public static class FieldNormalizationError extends Exception {
        private static final long serialVersionUID = 1L;
        
        public FieldNormalizationError(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
