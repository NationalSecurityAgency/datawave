package datawave.ingest.csv.mr.input;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.hash.UID;
import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeOverrideHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.input.reader.event.EventFixer;
import datawave.ingest.metadata.id.MetadataIdParser;
import datawave.ingest.validation.EventValidator;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;

public class CSVRecordReader extends CSVReaderBase implements EventFixer {

    private static final Logger log = Logger.getLogger(CSVRecordReader.class);

    private static final IngestConfiguration ingestConfig = IngestConfigurationFactory.getIngestConfiguration();

    protected String csvEventId;
    private final Multimap<String,Object> metadataForValidation = ArrayListMultimap.create(100, 1);
    private String rawData = null;

    private ExtendedCSVHelper csvHelper;
    private DataTypeOverrideHelper dataTypeHelper;
    private Map<String,String> securityMarkings;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        // call the super initialization
        super.initialize(genericSplit, context);
    }

    @Override
    public void initializeEvent(Configuration conf) throws IOException {
        // the data name over takes place here
        super.initializeEvent(conf);

        this.csvHelper = (ExtendedCSVHelper) this.helper;

        this.dataTypeHelper = new DataTypeOverrideHelper();
        this.dataTypeHelper.setup(conf);

        markingsHelper = ingestConfig.getMarkingsHelper(conf, this.csvHelper.getType());
    }

    @Override
    public void setup(Configuration conf) {
        try {
            initializeEvent(conf);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot setup CSVRecordReader", e);
        }
    }

    @Override
    public RawRecordContainer fixEvent(RawRecordContainer e) {
        rawFileName = e.getRawFileName();
        rawData = new String(e.getRawData());
        try {
            if (nextKeyValue()) {
                e = getEvent();
            }
        } catch (Exception ex) {
            e.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
        }
        return e;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (rawData != null) {
            setCounter(getCounter() + 1);
            return (getCounter() == 1);
        } else {
            return super.nextKeyValue();
        }
    }

    @Override
    public Text getCurrentValue() {
        if (rawData != null) {
            if (getCounter() == 1) {
                return new Text(rawData);
            } else {
                return null;
            }
        } else {
            return super.getCurrentValue();
        }
    }

    @Override
    protected ExtendedCSVHelper createHelper(Configuration conf) {
        return new ExtendedCSVHelper();
    }

    @Override
    public RawRecordContainer getEvent() {
        securityMarkings = null;
        csvEventId = null;
        metadataForValidation.clear();
        return super.getEvent();
    }

    @Override
    protected void decorateEvent() {
        if (null != this.securityMarkings && !this.securityMarkings.isEmpty()) {
            event.setSecurityMarkings(securityMarkings);
            try {
                event.setVisibility(MarkingFunctionsFactory.createMarkingFunctions().translateToColumnVisibility(securityMarkings));
            } catch (MarkingFunctions.Exception e) {
                log.error("Could not set default ColumnVisibility for the event", e);
                throw new RuntimeException(e);
            }
        }

        // now validate
        for (EventValidator validator : this.csvHelper.getValidators()) {
            validator.validate(event, metadataForValidation.asMap());
        }
    }

    /**
     * Determine if a field is required to be added to the metadata for validation
     *
     * @param fieldName
     *            field name
     * @return true if required
     */
    private boolean requiredForValidation(String fieldName) {
        if (metadataForValidation.containsKey(fieldName)) {
            return false;
        }
        for (EventValidator validator : this.csvHelper.getValidators()) {
            if (validator.validated(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void processPreSplitField(String fieldName, String fieldValue) {
        if (requiredForValidation(fieldName)) {
            metadataForValidation.put(fieldName, fieldValue);
        }
        super.processPreSplitField(fieldName, ExtendedCSVHelper.expandFieldValue(fieldValue));
    }

    @Override
    protected void checkField(String fieldName, String fieldValue) {
        if (requiredForValidation(fieldName)) {
            metadataForValidation.put(fieldName, fieldValue);
        }

        // If fieldName is a security marking field (as configured by EVENT_SECURITY_MARKING_FIELD_NAMES),
        // then put the marking value into this.securityMarkings, where the key is the field name for the marking
        // (as configured by EVENT_SECURITY_MARKING_FIELD_DOMAINS)
        if (this.csvHelper.getSecurityMarkingFieldDomainMap().containsKey(fieldName)) {
            if (null == this.securityMarkings) {
                this.securityMarkings = new HashMap<>();
            }
            if (!StringUtils.isEmpty(fieldValue)) {
                this.securityMarkings.put(this.csvHelper.getSecurityMarkingFieldDomainMap().get(fieldName), fieldValue);
            }
        }
        // Now lets add metadata extracted from the parsers
        else if (!StringUtils.isEmpty(this.csvHelper.getEventIdFieldName()) && fieldName.equals(this.csvHelper.getEventIdFieldName())) {

            if (this.csvHelper.getEventIdDowncase()) {
                fieldValue = fieldValue.toLowerCase();
            }

            // remember the id for uid creation
            csvEventId = fieldValue;

            try {
                getMetadataFromParsers(csvEventId);
            } catch (Exception e) {
                log.error("Error parsing id for metadata", e);
            }
        }
        // if we set the date with id, don't overwrite it
        if (!(fieldName.equals(eventDateFieldName) && event.isTimestampSet())) {
            super.checkField(fieldName, fieldValue);
        }

        dataTypeHelper.updateEventDataType(event, fieldName, fieldValue);
    }

    /**
     * Overridden to create a UID with appropriate extra attachment info
     */
    @Override
    protected UID uidOverride(RawRecordContainer e) {
        return DataTypeOverrideHelper.getUid(this.csvEventId, e.getTimeForUID(), uidBuilder);
    }

    /**
     * Apply the id metadata parser
     *
     * @param idFieldValue
     *            id field value
     * @throws Exception
     *             if there is an issue
     */
    protected void getMetadataFromParsers(String idFieldValue) throws Exception {
        Multimap<String,String> metadata = HashMultimap.create();
        for (Entry<String,MetadataIdParser> entry : this.csvHelper.getParsers().entries()) {
            entry.getValue().addMetadata(event, metadata, idFieldValue);
        }
        for (Map.Entry<String,String> entry : metadata.entries()) {
            checkField(entry.getKey(), entry.getValue());
        }
    }

    public MarkingsHelper getMarkingsHelper() {
        return markingsHelper;
    }

    public void setMarkingsHelper(MarkingsHelper markingHelper) {
        this.markingsHelper = markingHelper;
    }

}
