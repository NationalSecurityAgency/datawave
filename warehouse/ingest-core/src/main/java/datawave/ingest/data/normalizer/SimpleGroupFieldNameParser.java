package datawave.ingest.data.normalizer;

import java.util.Map.Entry;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Consolidated logic from SimpleGroupNameNormalizer and others.
 */
public class SimpleGroupFieldNameParser {
    private static final long serialVersionUID = 1035918631638323565L;
    private static final Logger log = Logger.getLogger(SimpleGroupFieldNameParser.class);
    private final boolean shouldIncludeDelimInGroupName;
    
    /**
     * 
     * @param shouldIncludeDelimInGroupName
     *            flag to set
     */
    public SimpleGroupFieldNameParser(boolean shouldIncludeDelimInGroupName) {
        this.shouldIncludeDelimInGroupName = shouldIncludeDelimInGroupName;
    }
    
    /**
     * @param origField
     *            the NormalizedContentInterface potentially containing a composite field name
     * @return a NormalizedContentInterface that contains a modified indexedFieldName and group, extracted from the original indexed field name
     */
    public NormalizedContentInterface extractFieldNameComponents(NormalizedContentInterface origField) {
        String originalIndexedFieldName = origField.getIndexedFieldName();
        String normParts[] = StringUtils.split(originalIndexedFieldName, '.');
        int index = originalIndexedFieldName.indexOf('.');
        
        if (index > -1) {
            String baseFieldName = originalIndexedFieldName.substring(0, index);
            String group = originalIndexedFieldName.substring(index + (shouldIncludeDelimInGroupName ? 0 : 1));
            
            NormalizedFieldAndValue revisedField = new NormalizedFieldAndValue(origField);
            
            // we are nested
            revisedField.setGrouped(true);
            // and the group is the atom
            revisedField.setGroup(group);
            // reset the indexed origField name to be the base origField name
            revisedField.setIndexedFieldName(baseFieldName);
            
            return revisedField;
        } else {
            return origField;
        }
    }
    
    /**
     * See {@link #extractFieldNameComponents(datawave.ingest.data.config.NormalizedContentInterface)}
     * 
     * @param fields
     *            list of fields to extract from
     * @return map of field name components
     */
    public Multimap<String,NormalizedContentInterface> extractFieldNameComponents(Multimap<String,NormalizedContentInterface> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        // if handling all fields, then we need to navigate the entire list
        for (Entry<String,NormalizedContentInterface> entry : fields.entries()) {
            NormalizedContentInterface field = entry.getValue();
            if (field != null) {
                NormalizedContentInterface revisedField = field;
                try {
                    revisedField = extractFieldNameComponents(field);
                } catch (Exception e) {
                    log.error("Failed to extract field name components: " + field.getIndexedFieldName() + '=' + field.getIndexedFieldValue(), e);
                    revisedField.setError(e);
                }
                results.put(revisedField.getIndexedFieldName(), revisedField);
            }
        }
        return results;
    }
}
