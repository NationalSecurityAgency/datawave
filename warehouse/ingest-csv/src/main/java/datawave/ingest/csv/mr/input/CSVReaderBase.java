package datawave.ingest.csv.mr.input;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.CSVHelper;
import datawave.data.hash.UID;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.input.reader.LongLineEventRecordReader;
import datawave.ingest.data.RawDataErrorNames;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * RecordReader that reads events from "Comma"-Separated-Value formats. Here the "Comma" can be any separator.
 */
public class CSVReaderBase extends LongLineEventRecordReader implements EventRecordReader {
    /** Logging mechanism for CSVReader. */
    private static final Logger log = LoggerFactory.getLogger(CSVReaderBase.class);
    
    /** Tracks the current record count. */
    protected long counter;
    
    /** The last time the input file was changed. */
    protected long fileModificationTime;
    
    /** Tracks the amount of data processed. */
    private long processedSize;
    
    /** The size of the InputSplit * 4. */
    private long totalSize;
    
    /** Primary DataTypeHelper for CSV records. */
    private CSVHelper csvHelper;
    
    /** Splits raw input records Strings according to the configured separator. */
    private StrTokenizer _tokenizer;
    
    /** Super class returns the position in bytes in the file as the key. This returns the record number. */
    @Override
    public LongWritable getCurrentKey() {
        key.set(counter);
        return key;
    }
    
    /** Points the RecordReader to the next record. */
    @Override
    public boolean nextKeyValue() throws IOException {
        if (counter == 0 && csvHelper.skipHeaderRow())
            super.nextKeyValue();
        counter++;
        
        return super.nextKeyValue();
    }
    
    @Override
    public void setInputDate(final long time) {
        fileModificationTime = time;
    }
    
    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        super.initialize(genericSplit, context);
        setInputDate(System.currentTimeMillis());
        initializeRawFileName(genericSplit);
        initializeTotalSize(genericSplit);
    }
    
    public void initializeRawFileName(final InputSplit genericSplit) {
        if (genericSplit instanceof FileSplit) {
            final FileSplit fs = (FileSplit) genericSplit;
            rawFileName = fs.getPath().getName();
        }
    }
    
    public void initializeTotalSize(final InputSplit genericSplit) throws IOException {
        try {
            totalSize = genericSplit.getLength() * 4l;
        } catch (InterruptedException ex) {
            throw new IOException("Interrupted Exception thrown while attempting to get split length", ex);
        }
    }
    
    /**
     * Gets the Event and this RecordReader ready for reading.
     * 
     * @param conf
     *            the configuration
     */
    @Override
    public void initializeEvent(Configuration conf) throws IOException {
        super.initializeEvent(conf);
        setCsvHelper((CSVHelper) helper);
        initializeTokenizer();
    }
    
    public void initializeTokenizer() {
        _tokenizer = createTokenizer();
    }
    
    /**
     * Creates a CVSHelper for the RecordReader.
     * 
     * @param conf
     *            a configuration
     * @return a CSVHelper object
     */
    @Override
    protected CSVHelper createHelper(Configuration conf) {
        return new CSVHelper();
    }
    
    /** @return a populated Event object from the current key and value in this RecordReader. */
    @Override
    public RawRecordContainer getEvent() {
        for (final String uidOverrideField : uidOverrideFields.keySet()) {
            uidOverrideFields.put(uidOverrideField, null);
        }
        
        super.getEvent();
        
        processedSize += value.getLength();
        
        final String rawEventRecordStr = value.toString();
        _tokenizer.reset(rawEventRecordStr);
        
        final String[] rawEventFields = _tokenizer.getTokenArray();
        final String[] header = csvHelper.getHeader();
        
        // If the event date field name is not specified in the configuration, then set the event date to the file modification time.
        if (StringUtils.isEmpty(eventDateFieldName))
            event.setDate(fileModificationTime);
        
        // We still try to process the event record.
        final int fields = Math.min(rawEventFields.length, header.length);
        String field, fieldName;
        int i;
        
        for (i = 0; i < fields; i++) {
            field = StringEscapeUtils.unescapeCsv(rawEventFields[i]);
            fieldName = header[i];
            
            field = csvHelper.clean(fieldName, field);
            if (field != null)
                processPreSplitField(fieldName, field);
        }
        
        // Check to see if we have data beyond the header specification that should be processed. This is the case for the CSV logs
        if (csvHelper.processExtraFields() && rawEventFields.length > header.length) {
            while (i < rawEventFields.length) {
                processExtraField(rawEventFields[i]);
                i++;
            }
        }
        
        // decorate with additional data (used by overriding classes)
        decorateEvent();
        
        event.setRawData(rawEventRecordStr.getBytes());
        
        // Check to see if we need to override the UID. The use case for this is that some of the hashes are "enrichment" and the same
        // values will be loaded over and over again. By default, the UID is calculated on the raw byte[]
        final UID newUID = uidOverride(event);
        if (newUID != null) {
            event.setId(newUID);
        } else {
            event.generateId(null);
        }
        
        enforcePolicy(event);
        
        if (header.length > rawEventFields.length) {
            event.addError(RawDataErrorNames.NOT_ENOUGH_FIELDS);
            log.error("More fields in header than in data. Header fields: {}, data fields: {}", header.length, rawEventFields.length);
        } else if ((!csvHelper.processExtraFields()) && (header.length < rawEventFields.length)) {
            event.addError(RawDataErrorNames.TOO_MANY_FIELDS);
            log.error("More fields in data than in header. Header fields: {}, data fields: {}", header.length, rawEventFields.length);
        }
        
        return event;
    }
    
    /** Decorate the event with additional info post field processing but prior to event validation */
    protected void decorateEvent() { /* default is noop */}
    
    @Override
    protected void checkField(final String name, final String value) {
        super.checkField(name, value);
        
        if (csvHelper.isFieldRequired(name) && StringUtils.isEmpty(value)) {
            event.addError(RawDataErrorNames.MISSING_DATA_ERROR);
            log.error("Missing required field: {}", name);
        }
    }
    
    /**
     * Creates a new StrTokenizer based on the configuration.
     * 
     * @return a string tokenizer
     */
    private StrTokenizer createTokenizer() {
        final StrTokenizer tokenizer;
        
        if (csvHelper.getSeparator().equals(",")) {
            tokenizer = StrTokenizer.getCSVInstance();
        } else if (csvHelper.getSeparator().equals("\\t")) {
            tokenizer = StrTokenizer.getTSVInstance();
        } else {
            tokenizer = new StrTokenizer();
            tokenizer.setDelimiterString(csvHelper.getSeparator());
        }
        
        tokenizer.setIgnoreEmptyTokens(false);
        tokenizer.setEmptyTokenAsNull(true);
        
        return tokenizer;
    }
    
    /**
     * Used to process extra fields. The PROCESS_EXTRA_FIELDS configuration parameter must be set to enable this processing.
     *
     * @param fieldValue
     *            field value
     */
    protected void processExtraField(String fieldValue) {
        if (fieldValue == null) {
            event.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
            log.error("processExtraField() called on a null value");
            return;
        }
        
        final int equalsIndex = fieldValue.indexOf('=');
        
        if (equalsIndex > 0) {
            String fieldName = fieldValue.substring(0, equalsIndex);
            fieldValue = fieldValue.substring(equalsIndex + 1);
            fieldValue = csvHelper.clean(fieldName, fieldValue);
            
            if (fieldValue != null)
                processPreSplitField(fieldName, fieldValue);
        } else {
            event.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
            log.error("Unable to process the following as a name=value pair: " + fieldValue);
        }
    }
    
    /**
     * Process a field. This will split multi-valued fields as necessary and call checkField on each part.
     *
     * @param fieldName
     *            field name
     * @param fieldValue
     *            field value
     */
    protected void processPreSplitField(String fieldName, final String fieldValue) {
        if (csvHelper.isMultiValuedField(fieldName)) {
            // Value can be multiple parts, need to break on semi-colon
            final String[] values = fieldValue.split(csvHelper.getEscapeSafeMultiValueSeparatorPattern());
            
            // Can be renamed if specified in multivalued fields, but not if using blacklist
            if (!csvHelper.usingMultiValuedFieldsBlacklist()) {
                fieldName = csvHelper.getMultiValuedFields().get(fieldName);
            }
            
            for (String value : values) {
                value = csvHelper.clean(fieldName, value);
                if (value != null)
                    checkField(fieldName, value);
            }
        } else {
            checkField(fieldName, fieldValue);
        }
    }
    
    public long getFileModificationTime() {
        return fileModificationTime;
    }
    
    public void setFileModificationTime(long fileModificationTime) {
        this.fileModificationTime = fileModificationTime;
    }
    
    @Override
    public float getProgress() {
        return Math.min(1f, (float) processedSize / (float) totalSize);
    }
    
    public StrTokenizer getTokenizer() {
        return _tokenizer;
    }
    
    public void setTokenizer(StrTokenizer _tokenizer) {
        this._tokenizer = _tokenizer;
    }
    
    public CSVHelper getCsvHelper() {
        return csvHelper;
    }
    
    public void setCsvHelper(CSVHelper csvHelper) {
        this.csvHelper = csvHelper;
    }
    
    public long getTotalSize() {
        return totalSize;
    }
    
    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }
    
    public long getCounter() {
        return counter;
    }
    
    public void setCounter(long counter) {
        this.counter = counter;
    }
    
    public long getProcessedSize() {
        return processedSize;
    }
    
    public void setProcessedSize(long processedSize) {
        this.processedSize = processedSize;
    }
}
