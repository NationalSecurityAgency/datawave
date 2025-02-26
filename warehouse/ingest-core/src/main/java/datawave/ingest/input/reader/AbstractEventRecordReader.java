package datawave.ingest.input.reader;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.policy.Policy;

public abstract class AbstractEventRecordReader<K> extends RecordReader<LongWritable,K> implements EventRecordReader {

    protected static final IngestConfiguration INGEST_CONFIG = IngestConfigurationFactory.getIngestConfiguration();

    private static final Logger logger = Logger.getLogger(AbstractEventRecordReader.class);

    protected final TreeMap<String,String> uidOverrideFields = new TreeMap<>();

    protected final RawRecordContainer event = INGEST_CONFIG.createRawRecordContainer();

    protected DataTypeHelper helper;

    protected CompressionCodecFactory compressionCodecs;

    /**
     * @deprecated to support the use of multiple formatters to parse a plurality of simple date formats
     */
    protected SimpleDateFormat formatter;
    protected List<SimpleDateFormat> formatters;

    protected MarkingsHelper markingsHelper;

    protected Set<String> uuidFields;

    protected String rawFileName;
    protected String eventDateFieldName;

    protected long rawFileTimeStamp;
    protected long inputDate;
    protected IngestPolicyEnforcer policyEnforcer;

    // Create a default UID builder (presumably hash-based)
    protected UIDBuilder<UID> uidBuilder = UID.builder();

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        initializeEvent(context.getConfiguration());

        if (genericSplit instanceof FileSplit) {
            final Path p = ((FileSplit) genericSplit).getPath();
            final FileSystem sys = p.getFileSystem(context.getConfiguration());

            rawFileName = p.toString();
            rawFileTimeStamp = sys.getFileStatus(p).getModificationTime();
        }
    }

    public void initializeEvent(final Configuration conf) throws IOException {
        // see if we have a data type override
        final String dataName = conf.get(Properties.DATA_NAME_OVERRIDE);
        if (dataName != null)
            conf.set(Properties.DATA_NAME, dataName);

        // In case the type registry is not already setup
        TypeRegistry.getInstance(conf);

        compressionCodecs = new CompressionCodecFactory(conf);

        helper = createHelper(conf);
        helper.setup(conf);

        // Get the event date field, its optional. If specified, get the format
        eventDateFieldName = conf.get(helper.getType().typeName() + Properties.EVENT_DATE_FIELD_NAME);

        if (!StringUtils.isEmpty(eventDateFieldName)) {
            String eventDateFieldFormat = conf.get(helper.getType().typeName() + Properties.EVENT_DATE_FIELD_FORMAT);
            String[] eventDateFieldFormats = conf.getStrings(helper.getType().typeName() + Properties.EVENT_DATE_FIELD_FORMATS);

            if (eventDateFieldFormat == null && eventDateFieldFormats == null) {
                throw new IllegalArgumentException("Neither " + helper.getType().typeName() + Properties.EVENT_DATE_FIELD_FORMAT + " nor "
                                + helper.getType().typeName() + Properties.EVENT_DATE_FIELD_FORMATS + " was specified");
            } else if (eventDateFieldFormat != null && eventDateFieldFormats != null) {
                throw new IllegalArgumentException("Both " + helper.getType().typeName() + Properties.EVENT_DATE_FIELD_FORMAT + " and "
                                + helper.getType().typeName() + Properties.EVENT_DATE_FIELD_FORMATS + " must not be specified, choose one");
            }

            if (eventDateFieldFormat != null) {
                formatter = new SimpleDateFormat(eventDateFieldFormat);
            }

            if (eventDateFieldFormats != null) { // getStrings will change 0 length strings to nulls
                formatters = new ArrayList<>(eventDateFieldFormats.length);
                for (String fmt : eventDateFieldFormats) {
                    formatters.add(new SimpleDateFormat(fmt));
                }
            }
        }

        initializeEventMarkingsHelper(conf, helper);

        final String[] uidFields = conf.getStrings(helper.getType().typeName() + Properties.EVENT_UID_FIELDS);
        if (uidFields != null)
            for (final String field : uidFields)
                uidOverrideFields.put(field, null);

        final String[] uuidFieldSet = conf.getStrings(helper.getType().typeName() + Properties.EVENT_UUID_FIELDS);
        if (uuidFieldSet != null)
            uuidFields = Sets.newHashSet(uuidFieldSet);

        if (Configurable.class.isAssignableFrom(event.getClass())) {
            ((Configurable) event).setConf(conf);
        }

        this.policyEnforcer = helper.getPolicyEnforcer();

        // Update the UID builder based on the configuration
        uidBuilder = UID.builder(conf);
    }

    protected void initializeEventMarkingsHelper(Configuration conf, DataTypeHelper helper) throws IOException {
        markingsHelper = INGEST_CONFIG.getMarkingsHelper(conf, helper.getType());
    }

    @Override
    public synchronized void close() throws IOException {
        // no op
    }

    protected DataTypeHelper createHelper(final Configuration conf) {
        return new DataTypeHelperImpl();
    }

    @Override
    public void setInputDate(final long time) {
        this.inputDate = time;
    }

    /**
     * This implementation of getEvent is not complete. This only creates an initial event that should be modified futher and validated.
     */
    @Override
    public RawRecordContainer getEvent() {
        event.clear();
        event.setDataType(helper.getType());
        event.setRawFileName(rawFileName);
        event.setRawFileTimestamp(rawFileTimeStamp);

        try {
            event.setRawRecordNumber(getCurrentKey().get());
        } catch (Exception e) {
            throw new RuntimeException("Unable to get current key", e);
        }

        setDefaultSecurityMarkings(event);

        return event;
    }

    protected void setDefaultSecurityMarkings(RawRecordContainer event) {
        event.setSecurityMarkings(markingsHelper.getDefaultMarkings());
    }

    /**
     * Ability to override the UID value. This is useful for datatypes where we want the UID to be based off the configured id field's value instead of the
     * entire record, so that the csv records and bud file content are merged into one event in the shard table. For the enrichment data, we want to base the
     * UID off of the MD5 hash and some other metadata, but not the dates in the record. This is because we will have to reload the enrichment data on a regular
     * basis and we want the same hashes to merge.
     *
     * @param event
     *            the event container to examine
     * @return the UID for the event
     */
    protected UID uidOverride(final RawRecordContainer event) {
        if (this.uidOverrideFields.isEmpty()) {
            return null;
        }

        final StringBuilder builder = new StringBuilder();
        for (final String value : uidOverrideFields.values()) {
            builder.append(value);
        }

        return uidBuilder.newId(builder.toString().getBytes(), event.getTimeForUID());
    }

    /**
     * Process a name, value pair and add to the event appropriately
     *
     * @param fieldName
     *            name of the field to check
     * @param fieldValue
     *            value of the field to check
     */
    protected void checkField(final String fieldName, final String fieldValue) {
        if (!StringUtils.isEmpty(eventDateFieldName) && fieldName.equals(eventDateFieldName)) {
            extractEventDate(fieldName, fieldValue);
        } else if (uidOverrideFields.containsKey(fieldName)) {
            uidOverrideFields.put(fieldName, fieldValue);
        } else if (uuidFields != null && uuidFields.contains(fieldName)) {
            event.getAltIds().add(fieldValue);
        }
    }

    protected void extractEventDate(final String fieldName, final String fieldValue) {
        if (formatters != null && formatter != null) {
            throw new IllegalStateException("Both 'formatter' and 'formatters' are set. Please define a single event date mechanism");
        } else if (formatters != null) {
            for (SimpleDateFormat format : formatters) {
                try {
                    event.setDate(format.parse(DateNormalizer.convertMicroseconds(fieldValue, format.toPattern())).getTime());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Parsed date from '" + fieldName + "' using formatter " + format.toPattern());
                    }
                    break;
                } catch (java.text.ParseException e) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Error parsing date from hash record using format " + format.toPattern(), e);
                    }
                }
            }
            if (!event.isTimestampSet()) {
                List<String> patterns = new ArrayList<>(formatters.size());
                for (SimpleDateFormat formatter : formatters) {
                    patterns.add(formatter.toPattern());
                }
                logger.error("Unable to parse date '" + fieldValue + "' from field '" + fieldName + " using formatters " + patterns);
            }
        } else if (formatter != null) {
            try {
                event.setDate(formatter.parse(DateNormalizer.convertMicroseconds(fieldValue, formatter.toPattern())).getTime());
            } catch (java.text.ParseException e) {
                logger.error("Error parsing date from hash record", e);
            }
        } else {
            throw new IllegalArgumentException("No event data formatters are set");
        }
    }

    @Override
    public String getRawInputFileName() {
        return rawFileName;
    }

    @Override
    public long getRawInputFileTimestamp() {
        return rawFileTimeStamp;
    }

    @Override
    public RawRecordContainer enforcePolicy(RawRecordContainer event) {
        if (null != event) {
            if (null != policyEnforcer) { // Must have a policy enforcer, even if it is a no-op
                try {
                    this.policyEnforcer.validate(event);
                } catch (Policy.Exception e) {
                    logger.error(e.getMessage());
                    event.getErrors().add(RawDataErrorNames.POLICY_ENFORCER_EXCEPTION);
                }
            } else {
                event.getErrors().add(RawDataErrorNames.MISSING_POLICY_ENFORCER);
            }
        }
        return event;
    }

    public IngestPolicyEnforcer getPolicyEnforcer() {
        return policyEnforcer;
    }

    public void setPolicyEnforcer(IngestPolicyEnforcer policyEnforcer) {
        this.policyEnforcer = policyEnforcer;
    }
}
