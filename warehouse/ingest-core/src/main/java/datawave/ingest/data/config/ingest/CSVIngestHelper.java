package datawave.ingest.data.config.ingest;

import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.util.StringUtils;

public class CSVIngestHelper extends ContentBaseIngestHelper {

    private static final Logger log = Logger.getLogger(CSVIngestHelper.class);
    protected CSVHelper helper = null;

    @Override
    public void setup(Configuration config) {
        super.setup(config);
        helper = createHelper();
        helper.setup(config);
        this.setEmbeddedHelper(helper);
    }

    /**
     * Create the helper for this class to use. Can be overriden to supply an alternate helper class
     *
     * @return a csvhelper
     */
    protected CSVHelper createHelper() {
        return new CSVHelper();
    }

    /**
     * Allow classes extending this class to modify the StrTokenizer being used.
     *
     * @param tokenizer
     *            The StrTokenizer that will be used on each Event
     * @return the tokenizer to be used
     */
    protected StrTokenizer configureTokenizer(StrTokenizer tokenizer) {
        return tokenizer;
    }

    /**
     * Allow classes extending this class to modify the raw data before setting it on the StrTokenizer
     *
     * @param data
     *            The raw data from the Event
     * @return the raw data in String form
     */
    protected String preProcessRawData(byte[] data) {
        return new String(data);
    }

    /**
     * This method uses the header and the csv string in raw bytes of the Event to create key value pairs.
     *
     * @param event
     *            the event
     * @return map of event fields
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        HashMultimap<String,String> fields = HashMultimap.create();

        String data = preProcessRawData(event.getRawData());

        StrTokenizer tokenizer;
        if (helper.getSeparator().equals(","))
            tokenizer = StrTokenizer.getCSVInstance();
        else if (helper.getSeparator().equals("\\t"))
            tokenizer = StrTokenizer.getTSVInstance();
        else
            tokenizer = new StrTokenizer(data, helper.getSeparator());

        tokenizer.setIgnoreEmptyTokens(false);
        tokenizer.setEmptyTokenAsNull(true);

        // Allow subclasses to override the tokenizer
        tokenizer = configureTokenizer(tokenizer);

        tokenizer.reset(data);

        String[] dataFields = tokenizer.getTokenArray();
        processFields(fields, dataFields);

        // and return the normalized fields
        return normalize(fields);
    }

    protected void processFields(HashMultimap<String,String> fields, String[] dataFields) {
        for (int i = 0; i < Math.max(dataFields.length, helper.getHeader().length); i++) {

            if (i < helper.getHeader().length) {
                String fieldName = helper.getHeader()[i];

                if (keepField(fieldName) && dataFields[i] != null) {
                    String fieldValue = StringEscapeUtils.unescapeCsv(dataFields[i]);
                    fieldValue = helper.clean(fieldName, fieldValue);
                    if (fieldValue != null) {
                        processPreSplitField(fields, fieldName, fieldValue);
                    }
                }
            } else if (helper.processExtraFields()) {
                // We have gone beyond the length of the header. In some cases,
                // this will contain optional fields in the form of a map.
                // Split on equals, to break the key and value
                String fieldValue = StringEscapeUtils.unescapeCsv(dataFields[i]);
                if (fieldValue != null) {
                    processExtraField(fields, fieldValue);
                }
            } else {
                break;
            }

        }
    }

    /**
     * Used to process extra fields. The PROCESS_EXTRA_FIELDS configuration parameter must be set to enable this processing.
     *
     * @param fields
     *            extra fields to process
     * @param fieldValue
     *            the field value
     */
    protected void processExtraField(Multimap<String,String> fields, String fieldValue) {
        int equalsIndex = -1;
        if (fieldValue != null) {
            equalsIndex = fieldValue.indexOf('=');
        }
        if (equalsIndex > 0) {
            String fieldName = fieldValue.substring(0, equalsIndex);

            if (keepField(fieldName)) {
                fieldValue = fieldValue.substring(equalsIndex + 1);
                fieldValue = helper.clean(fieldName, fieldValue);
                if (fieldValue != null) {
                    processPreSplitField(fields, fieldName, fieldValue);
                }
            }
        } else {
            log.error("Unable to process the following as a name=value pair: " + fieldValue);
        }
    }

    /**
     * Process a field. This will split multi-valued fields as necessary and call processField on each part.
     *
     * @param fields
     *            list of fields
     * @param fieldName
     *            name of the field
     * @param fieldValue
     *            value of the field
     */
    protected void processPreSplitField(Multimap<String,String> fields, String fieldName, String fieldValue) {
        if (fieldValue != null) {
            if (helper.isMultiValuedField(fieldName)) {
                // Value can be multiple parts, need to break on semi-colon
                String singleFieldName = helper.usingMultiValuedFieldsDisallowlist() ? fieldName : helper.getMultiValuedFields().get(fieldName);
                int limit = helper.getMultiFieldSizeThreshold();
                int count = 0;
                for (String value : StringUtils.splitIterable(fieldValue, helper.getEscapeSafeMultiValueSeparatorPattern())) {
                    value = helper.clean(singleFieldName, value);
                    if (value != null) {
                        if (count == limit) {
                            applyMultiValuedThresholdAction(fields, fieldName, singleFieldName);
                            break;
                        } else {
                            processField(fields, singleFieldName, value);
                            count++;
                        }
                    }
                }
            } else {
                processField(fields, fieldName, fieldValue);
            }
        }
    }

    protected void applyThresholdAction(Multimap<String,String> fields, String fieldName, String value, int sizeLimit) {
        switch (helper.getThresholdAction()) {
            case DROP:
                processField(fields, helper.getDropField(), aliaser.normalizeAndAlias(fieldName));
                break;
            case REPLACE:
                processField(fields, fieldName, helper.getThresholdReplacement());
                break;
            case TRUNCATE:
                processField(fields, fieldName, value.substring(0, sizeLimit));
                processField(fields, helper.getTruncateField(), aliaser.normalizeAndAlias(fieldName));
                break;
            case FAIL:
                throw new IllegalArgumentException("A field : " + fieldName + " was too large to process");
        }
    }

    protected void applyMultiValuedThresholdAction(Multimap<String,String> fields, String fieldName, String singleFieldName) {
        switch (helper.getThresholdAction()) {
            case DROP:
                if (singleFieldName != null) {
                    fields.removeAll(singleFieldName);
                }
                processField(fields, helper.getMultiValuedDropField(), aliaser.normalizeAndAlias(fieldName));
                break;
            case REPLACE:
                if (singleFieldName != null) {
                    fields.removeAll(singleFieldName);
                }
                processField(fields, fieldName, helper.getMultiValuedThresholdReplacement());
                break;
            case TRUNCATE:
                processField(fields, helper.getMultiValuedTruncateField(), aliaser.normalizeAndAlias(fieldName));
                break;
            case FAIL:
                throw new IllegalArgumentException("A field : " + fieldName + " was too large to process");
        }
    }

    /**
     * Process a name, value pair and add to the event fields
     *
     * @param fields
     *            list of fields
     * @param fieldName
     *            name of the field
     * @param fieldValue
     *            value of the field
     */
    protected void processField(Multimap<String,String> fields, String fieldName, String fieldValue) {
        int sizeLimit = helper.getFieldSizeThreshold();
        if (fieldValue.length() > sizeLimit) {
            applyThresholdAction(fields, fieldName, fieldValue, sizeLimit);
        } else {
            fieldValue = helper.cleanEscapedMultivalueSeparators(fieldValue);
            fields.put(fieldName, fieldValue);
        }
    }

    /**
     * Test whether the field should be kept by checking against the disallowlist and allowlist. Presence in the disallowlist takes precedence over presence on
     * the allowlist.
     *
     * @param fieldName
     *            the field name
     * @return whether field should be kept or not
     */
    protected boolean keepField(String fieldName) {
        final Set<String> disallowlist = helper.getFieldDisallowlist();
        final Set<String> allowlist = helper.getFieldAllowlist();

        if (disallowlist != null && disallowlist.contains(fieldName)) {
            return false; // drop the field that is in the disallowlist.
        } // else keep the non-disallowlisted field.

        if (allowlist != null) { // allowlist exists
            if (!allowlist.contains(fieldName)) {
                return false; // drop the field not in the allowlist.
            } // else keep the allowlisted field.
        } // else keep field.

        return true;
    }
}
