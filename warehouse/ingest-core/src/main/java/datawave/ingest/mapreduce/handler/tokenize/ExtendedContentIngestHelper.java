package datawave.ingest.mapreduce.handler.tokenize;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import datawave.data.type.Type;
import datawave.ingest.metadata.id.MetadataIdParser;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.EventFieldNormalizerHelper;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class ExtendedContentIngestHelper extends BaseIngestHelper implements TermFrequencyIngestHelperInterface {
    
    private static final Logger log = Logger.getLogger(ExtendedContentIngestHelper.class);
    
    private static final String UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}.*";
    private static final int UUID_LENGTH = 36;
    
    private ExtendedContentDataTypeHelper helper;
    private Set<String> zones = new HashSet<>();
    private EventFieldNormalizerHelper eventFieldNormalizerHelper = null;
    
    public ExtendedContentDataTypeHelper getHelper() {
        return this.helper;
    }
    
    @Override
    public void setup(Configuration config) {
        super.setup(config);
        helper = new ExtendedContentDataTypeHelper(this.getType().typeName());
        helper.setup(config);
        this.setEmbeddedHelper(helper);
        this.eventFieldNormalizerHelper = new EventFieldNormalizerHelper(config);
    }
    
    public void addField(Multimap<String,NormalizedContentInterface> fields, String name, String value) {
        if (value != null && !value.isEmpty()) {
            NormalizedFieldAndValue n = new NormalizedFieldAndValue(name, value);
            fields.put(name, n);
        }
    }
    
    public void addTokenField(Multimap<String,NormalizedContentInterface> fields, String name) {
        // add the field to the map of fields (value really doesn't matter, so
        // reuse name as value here)
        addField(fields, name, name);
        // remember this field as one of our zones
        this.zones.add(name);
    }
    
    /**
     * Add the metadata from the event into our event fields
     * 
     * @param from
     *            the from event fields
     * @param to
     *            the to event fields
     */
    protected void addMetadata(Map<String,Collection<Object>> from, Multimap<String,NormalizedContentInterface> to) {
        if (from == null)
            return;
        
        for (Map.Entry<String,Collection<Object>> v : from.entrySet()) {
            String key = v.getKey().toString();
            String fieldName = key.toUpperCase();
            
            int sizeLimit = helper.getFieldSizeThreshold();
            int countLimit = helper.getMultiFieldSizeThreshold();
            int count = 0;
            
            // the multi-valued fields config specifies a plural to singular
            // field name mapping in some cases.
            String singleFieldName = fieldName;
            if (helper.getMultiValuedFields().containsKey(fieldName)) {
                singleFieldName = helper.getMultiValuedFields().get(fieldName);
            }
            
            for (Object o : v.getValue()) {
                if (helper.isValidMetadataKey(key) && o != null) {
                    String value = helper.clean(fieldName, o.toString());
                    value = helper.clean(singleFieldName, value);
                    
                    if (value != null) {
                        if (count == countLimit) {
                            applyMultiValuedThresholdAction(to, fieldName, singleFieldName);
                            break;
                        } else if (value.length() > sizeLimit) {
                            applyThresholdAction(to, singleFieldName, value, sizeLimit);
                        } else {
                            to.put(singleFieldName, new NormalizedFieldAndValue(singleFieldName, value));
                            count++;
                        }
                    }
                }
            }
        }
    }
    
    protected void applyThresholdAction(Multimap<String,NormalizedContentInterface> fields, String fieldName, String value, int sizeLimit) {
        switch (helper.getThresholdAction()) {
            case DROP:
                fields.put(helper.getDropField(), new NormalizedFieldAndValue(helper.getDropField(), aliaser.normalizeAndAlias(fieldName)));
                break;
            case REPLACE:
                fields.put(fieldName, new NormalizedFieldAndValue(fieldName, helper.getThresholdReplacement()));
                break;
            case TRUNCATE:
                fields.put(fieldName, new NormalizedFieldAndValue(fieldName, value.substring(0, sizeLimit)));
                fields.put(helper.getTruncateField(), new NormalizedFieldAndValue(helper.getTruncateField(), aliaser.normalizeAndAlias(fieldName)));
                break;
            case FAIL:
                throw new IllegalArgumentException("A field : " + fieldName + " was too large to process");
        }
    }
    
    protected void applyMultiValuedThresholdAction(Multimap<String,NormalizedContentInterface> fields, String fieldName, String singleFieldName) {
        switch (helper.getMultiValuedThresholdAction()) {
            case DROP:
                if (singleFieldName != null) {
                    fields.removeAll(singleFieldName);
                }
                fields.put(helper.getMultiValuedDropField(),
                                new NormalizedFieldAndValue(helper.getMultiValuedDropField(), aliaser.normalizeAndAlias(fieldName)));
                break;
            case REPLACE:
                if (singleFieldName != null) {
                    fields.removeAll(singleFieldName);
                }
                fields.put(fieldName, new NormalizedFieldAndValue(fieldName, helper.getThresholdReplacement()));
                break;
            case TRUNCATE:
                fields.put(helper.getMultiValuedTruncateField(),
                                new NormalizedFieldAndValue(helper.getMultiValuedTruncateField(), aliaser.normalizeAndAlias(fieldName)));
                break;
            case FAIL:
                throw new IllegalArgumentException("A field : " + fieldName + " was too large to process");
        }
    }
    
    /**
     * Parse metadata base on the id
     * 
     * @param event
     *            the event container
     * @param id
     *            the event id
     */
    public void addMetadataFromId(RawRecordContainer event, String id) {
        Multimap<String,String> metadata = HashMultimap.create();
        for (MetadataIdParser parser : helper.getMetadataKeyParsers()) {
            try {
                parser.addMetadata(event, metadata, id);
            } catch (Exception e) {
                log.error("Unable to apply " + parser + " to " + id, e);
                event.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
            }
        }
    }
    
    public void inheritFromRootPayload(RawRecordContainer event, Map<String,Collection<Object>> metadata) {
        // TODO: No-op here, but need to refactor the following in upstream
        // if (metadata == null)
        // return;
        //
        // IBaseDataObject payload = (IBaseDataObject) event.getAuxData();
        // if (payload != null) {
        // for (String field : helper.getInheritedPayloadFields()) {
        // if (metadata.containsKey(field) && (!payload.hasParameter(field))) {
        // payload.putParameter(field, metadata.get(field));
        // }
        // }
        // }
    }
    
    public void addMetadataFromParms(RawRecordContainer event, Map<String,Collection<Object>> metadata, String id) {
        
        for (Map.Entry<String,Collection<Object>> entry : metadata.entrySet()) {
            CharSequence key = entry.getKey();
            for (Object value : entry.getValue()) {
                if (value == null)
                    continue;
                
                if (helper.getUuids().contains(key)) {
                    event.getAltIds().add(String.valueOf(value));
                }
                if (this.helper.getSecurityMarkingFieldDomainMap().containsKey(key)) {
                    addSecurityMetadataFromParms(key, value, event);
                }
            }
        }
        
        Multimap<String,String> newMetadata = HashMultimap.create();
        Multimap<String,MetadataIdParser> fieldParsers = helper.getMetadataFieldParsers();
        for (String field : fieldParsers.keySet()) {
            if (metadata.containsKey(field) && metadata.get(field) != null) {
                for (MetadataIdParser parser : fieldParsers.get(field)) {
                    for (Object v : metadata.get(field)) {
                        if (v == null)
                            continue;
                        try {
                            parser.addMetadata(event, newMetadata, v.toString());
                        } catch (Exception e) {
                            log.error("Unable to apply " + parser + " to " + field, e);
                            event.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
                        }
                    }
                }
            }
        }
        
        // If the ID is a UUID (i.e., contains no date), try to get
        // event metadata from the parameters
        if ((id != null) && (id.length() >= UUID_LENGTH) && id.matches(UUID_PATTERN)) {
            fieldParsers = helper.getMetadataFieldUuidParsers();
            for (String field : fieldParsers.keySet()) {
                if (metadata.containsKey(field) && metadata.get(field) != null) {
                    for (MetadataIdParser parser : fieldParsers.get(field)) {
                        for (Object v : metadata.get(field)) {
                            if (v == null)
                                continue;
                            try {
                                parser.addMetadata(event, newMetadata, v.toString());
                            } catch (Exception e) {
                                log.error("Unable to apply " + parser + " to " + field, e);
                                event.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
                            }
                        }
                    }
                }
            }
        }
    }
    
    protected void addSecurityMetadataFromParms(CharSequence key, Object value, RawRecordContainer event) {
        // If fieldName is a security marking field (as configured by EVENT_SECURITY_MARKING_FIELD_NAMES),
        // then put the marking value into this.securityMarkings, where 'key' maps to the domain for the marking
        // (as configured by EVENT_SECURITY_MARKING_FIELD_DOMAINS)
        if (!StringUtils.isEmpty(key.toString()) && !StringUtils.isEmpty(value.toString())) {
            event.addSecurityMarking(this.helper.getSecurityMarkingFieldDomainMap().get(key.toString()), value.toString());
        }
    }
    
    /**
     * Override the normalize call to enable event field value normalization
     * 
     * @return a set of the event content interface
     */
    public Set<NormalizedContentInterface> normalize(NormalizedContentInterface nci) {
        
        // normalize the event field value as required
        Type<?> n = eventFieldNormalizerHelper.getType(nci.getEventFieldName());
        try {
            nci.setEventFieldValue(n.normalize(nci.getEventFieldValue()));
            
            // copy the new value into the indexed value for further
            // normalization
            nci.setIndexedFieldValue(nci.getEventFieldValue());
        } catch (Exception e) {
            log.error("Failed to normalize " + nci.getEventFieldName() + '=' + nci.getEventFieldValue(), e);
            nci.setError(e);
        }
        
        // now normalize the index field value as required
        return super.normalize(nci);
    }
    
    /**
     * Make a wrapper around the aliaser's resolveAlias which is in the {@link BaseIngestHelper}. We don't maintain the Map of indexed content, so we want to
     * make sure the field name gets aliased (uppercased) properly.
     * 
     * @param nci
     *            the normalized content interface
     * @return the normalized content interface with proper aliases
     */
    public NormalizedContentInterface resolveAlias(NormalizedContentInterface nci) {
        return super.aliaser.normalizeAndAlias(nci);
    }
    
    @Override
    public boolean isTermFrequencyField(String field) {
        return zones.contains(field);
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        throw new UnsupportedOperationException("getEventFields is not supported. If needed, then provide subclass implementation");
    }
}
