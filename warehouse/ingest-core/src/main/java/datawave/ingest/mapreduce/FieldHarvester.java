package datawave.ingest.mapreduce;

import com.google.common.annotations.VisibleForTesting;
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

/**
 * Encapsulates the logic for extracting fields from a record, making use of a provided IngestHelperInterface. Generates virtual fields, composite fields, and
 * supplemental fields (like LOAD_DATE, ORIG_FILE, and RAW_FILE). Some logic for handling errors is also included here: extracting salvagable fields if any
 * exception occurs, and detecting if there were field errors (indicating a normalization failure).
 */
public class FieldHarvester {
    private static final Logger log = Logger.getLogger(FieldHarvester.class);
    
    private static Now now = Now.getInstance();
    
    public static final String LOAD_DATE_FIELDNAME = "LOAD_DATE";
    public static final String SEQUENCE_FILE_FIELDNAME = "ORIG_FILE";
    public static final String RAW_FILE_FIELDNAME = "RAW_FILE";
    public static final String LOAD_SEQUENCE_FILE_NAME = "ingest.event.mapper.load.seq.filename";
    public static final String TRIM_SEQUENCE_FILE_NAME = "ingest.event.mapper.trim.sequence.filename";
    public static final String LOAD_RAW_FILE_NAME = "ingest.event.mapper.load.raw.filename";
    
    private boolean createSequenceFileName;
    private boolean trimSequenceFileName;
    private boolean createRawFileName;
    private final DateNormalizer dateNormalizer = new DateNormalizer();
    
    private static final String SRC_FILE_DEL = "|";
    private Exception originalException;
    
    public FieldHarvester(Configuration configuration) {
        this.createSequenceFileName = configuration.getBoolean(LOAD_SEQUENCE_FILE_NAME, true);
        this.trimSequenceFileName = configuration.getBoolean(TRIM_SEQUENCE_FILE_NAME, true);
        this.createRawFileName = configuration.getBoolean(LOAD_RAW_FILE_NAME, true);
    }
    
    /**
     * Updates "fields" with extracted, derived, and automatically generated fields. Will capture
     * exception along the way and attempt to add salvaged fields before rethrowing the exception.
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
                    String splitStart) throws Exception {
        // reset exception-in-extraction tracking
        this.originalException = null;
        
        // "candidateFields" holds the fields that will eventually be added to "fields"
        Multimap<String,NormalizedContentInterface> candidateFields;
        
        try {
            // parse the record into its candidate field names and values using the IngestHelperInterface.
            candidateFields = ingestHelper.getEventFields(value);
        } catch (Exception exception) {
            // delay throwing the exception to attempt salvaging
            this.originalException = exception;
            candidateFields = attemptToSalvageFields(value, ingestHelper);
        }
        
        try {
            // try adding supplemental fields to candidateFields, whether or not they were salvaged
            addSupplementalFields(value, offset, splitStart, ingestHelper, candidateFields);
        } catch (Exception exception) {
            if (null == this.originalException) {
                this.originalException = exception;
            } else {
                // preserve original exception and log the latest exception
                log.error("A secondary exception occurred while adding supplemental fields", exception);
            }
        }
        
        // add candidateFields to fields, even if there was an error
        // identify if any individual fields contain an error
        addFieldsAndDetectFieldErrors(fields, candidateFields);
        
        if (null != this.originalException) {
            log.error("Rethrowing original exception after completing field extraction.");
            throw originalException;
        }
    }
    
    @VisibleForTesting
    boolean hasError() {
        return null != this.originalException;
    }
    
    @VisibleForTesting
    Exception getOriginalException() {
        return this.originalException;
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
                if (null == this.originalException) {
                    this.originalException = new IllegalStateException("Unexpected state (FieldExpander.exception should be non-null if salvaging",
                                    salvagerException);
                } else {
                    // allow original exception (this.exception) to be thrown by caller
                    log.error("Even salvager threw an exception", salvagerException);
                }
            }
        }
        return HashMultimap.create();
    }
    
    private void addSupplementalFields(RawRecordContainer value, long offset, String splitStart, IngestHelperInterface ingestHelper,
                    Multimap<String,NormalizedContentInterface> fields) {
        addVirtualFields(ingestHelper, fields);
        addCompositeFields(ingestHelper, fields);
        addLoadDateField(fields);
        addFileNameFields(value, offset, splitStart, fields);
        applyFieldFilters(ingestHelper, fields);
    }
    
    private void addVirtualFields(IngestHelperInterface ingestHelper, Multimap<String,NormalizedContentInterface> newFields) {
        // Also get the virtual fields, if applicable.
        if (null != newFields && ingestHelper instanceof VirtualIngest) {
            VirtualIngest vHelper = (VirtualIngest) ingestHelper;
            Multimap<String,NormalizedContentInterface> virtualFields = vHelper.getVirtualFields(newFields);
            for (Map.Entry<String,NormalizedContentInterface> v : virtualFields.entries())
                newFields.put(v.getKey(), v.getValue());
        }
    }
    
    private void addCompositeFields(IngestHelperInterface ingestHelper, Multimap<String,NormalizedContentInterface> newFields) {
        // Also get the composite fields, if applicable
        if (null != newFields && ingestHelper instanceof CompositeIngest) {
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
     * Adds candidateFields to fields. Looks at each of the candidate fields, inspection for field errors. Sets the field harvester's exception field if any
     * field errors were found.
     */
    private void addFieldsAndDetectFieldErrors(Multimap<String,NormalizedContentInterface> fields, Multimap<String,NormalizedContentInterface> candidateFields) {
        if (null == candidateFields) {
            return;
        }
        Throwable fieldError = null;
        for (Map.Entry<String,NormalizedContentInterface> entry : candidateFields.entries()) {
            // noinspection ThrowableResultOfMethodCallIgnored
            if (null != entry.getValue().getError()) {
                fieldError = entry.getValue().getError();
            }
            fields.put(entry.getKey(), entry.getValue());
        }
        if (null != fieldError) {
            if (null == this.originalException) {
                this.originalException = new FieldNormalizationError("Failed getting all fields", fieldError);
            } else {
                // preserve original exception
                log.error("A field exception was observed while adding fields", fieldError);
            }
        }
    }
    
    public static class FieldNormalizationError extends Exception {
        
        private static final long serialVersionUID = 1L;
        
        public FieldNormalizationError(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
