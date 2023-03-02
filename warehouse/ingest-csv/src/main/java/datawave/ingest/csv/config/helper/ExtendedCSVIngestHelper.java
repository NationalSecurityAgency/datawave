package datawave.ingest.csv.config.helper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.data.config.ingest.EventFieldNormalizerHelper;
import datawave.ingest.data.normalizer.SimpleGroupFieldNameParser;
import datawave.ingest.metadata.id.MetadataIdParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrMatcher;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ExtendedCSVIngestHelper extends CSVIngestHelper {
    
    private static final Logger log = Logger.getLogger(ExtendedCSVIngestHelper.class);
    
    protected ExtendedCSVHelper helper = null;
    private EventFieldNormalizerHelper eventFieldNormalizerHelper = null;
    private SimpleGroupFieldNameParser groupNormalizer = new SimpleGroupFieldNameParser(true);
    
    @Override
    public void setup(Configuration config) {
        super.setup(config);
        this.helper = (ExtendedCSVHelper) super.helper;
        
        // lets use an event field normalization helper
        eventFieldNormalizerHelper = new EventFieldNormalizerHelper(config);
    }
    
    @Override
    protected CSVHelper createHelper() {
        return new ExtendedCSVHelper();
    }
    
    @Override
    protected void processPreSplitField(Multimap<String,String> fields, String fieldName, String fieldValue) {
        // process all field upper case
        fieldName = fieldName.toUpperCase();
        fieldValue = ExtendedCSVHelper.expandFieldValue(fieldValue);
        super.processPreSplitField(fields, fieldName, fieldValue);
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        // applying groupNormalizer for csv data
        Multimap<String,NormalizedContentInterface> fields = super.getEventFields(event);
        
        // drop any field configured as such
        for (String field : this.helper.getIgnoredFields()) {
            fields.removeAll(field);
        }
        
        return fields;
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        
        for (Entry<String,String> e : fields.entries()) {
            if (e.getValue() != null) {
                // overriding this method, so we can apply groupNormalizer before other normalizers are applied
                applyNormalizationAndAddToResults(results, groupNormalizer.extractFieldNameComponents(new NormalizedFieldAndValue(e.getKey(), e.getValue())));
            } else
                log.warn(this.getType().typeName() + " has key " + e.getKey() + " with a null value.");
        }
        return results;
    }
    
    @Override
    protected void processField(Multimap<String,String> fields, String fieldName, String fieldValue) {
        
        // Add metadata extracted from the parsers
        if (!StringUtils.isEmpty(this.helper.getEventIdFieldName()) && fieldName.equals(this.helper.getEventIdFieldName())) {
            try {
                getMetadataFromParsers(fields, fieldValue);
            } catch (Exception e) {
                log.error("Error parsing id for metadata", e);
            }
        }
        
        super.processField(fields, fieldName, fieldValue);
    }
    
    /**
     * Apply the id metadata parser
     * 
     * @param idFieldValue
     *            the id field value
     * @param fields
     *            the event fields
     * @throws Exception
     *             if there is an issue
     */
    protected void getMetadataFromParsers(Multimap<String,String> fields, String idFieldValue) throws Exception {
        Multimap<String,String> metadata = HashMultimap.create();
        for (Entry<String,MetadataIdParser> entry : this.helper.getParsers().entries()) {
            entry.getValue().addMetadata(null, metadata, idFieldValue);
        }
        for (Map.Entry<String,String> entry : metadata.entries()) {
            processField(fields, entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Override the normalize call to enable event field value normalization
     * 
     * @param nci
     *            the normalized content interface
     */
    public Set<NormalizedContentInterface> normalize(NormalizedContentInterface nci) {
        
        // normalize the event field value as required
        Type<?> n = eventFieldNormalizerHelper.getType(nci.getEventFieldName());
        try {
            nci.setEventFieldValue(n.normalize(nci.getEventFieldValue()));
            
            // copy the new value into the indexed value for further normalization
            nci.setIndexedFieldValue(nci.getEventFieldValue());
        } catch (Exception e) {
            log.error("Failed to normalize " + nci.getEventFieldName() + '=' + nci.getEventFieldValue(), e);
            nci.setError(e);
        }
        
        // now normalize the index field value as required
        return super.normalize(nci);
    }
    
    @Override
    protected StrTokenizer configureTokenizer(StrTokenizer tokenizer) {
        // Remove the trim matcher, trim in preProcessRawData instead so
        // we don't lost any trailing whitespace on the last metadata pair
        // on the record
        return tokenizer.setTrimmerMatcher(StrMatcher.noneMatcher());
    }
    
    @Override
    protected String preProcessRawData(byte[] data) {
        String buf = new String(data);
        
        // Need to make sure we're operating on chars in case
        // we're manipulating multi-byte characters
        char[] chars = buf.toCharArray();
        
        // Trim all initial whitespace by skipping
        int index = 0;
        while (index < chars.length && chars[index] <= 32) {
            index++;
        }
        
        // If we trimmed the front, return a new String
        if (index > 0) {
            return new String(chars, index, chars.length - index);
        } else {
            // Otherwise, we don't have to
            return buf;
        }
    }
}
