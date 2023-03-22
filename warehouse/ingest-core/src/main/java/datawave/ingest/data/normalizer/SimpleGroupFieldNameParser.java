package datawave.ingest.data.normalizer;

import java.util.Map.Entry;
import java.util.regex.Pattern;

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
    private static final Pattern parentGroupingPattern = Pattern.compile("_\\d");
    private static final Pattern parentGroupingMatch = Pattern.compile(".*_\\d\\..*");
    
    public SimpleGroupFieldNameParser() {}
    
    /**
     * @param origField
     *            the NormalizedContentInterface potentially containing a composite field name
     * @return a NormalizedContentInterface that contains a modified indexedFieldName and group, extracted from the original indexed field name
     */
    public NormalizedContentInterface extractFieldNameComponents(NormalizedContentInterface origField) {
        String originalIndexedFieldName = origField.getIndexedFieldName();
        String[] splits = StringUtils.split(originalIndexedFieldName, '.');
        int index = originalIndexedFieldName.indexOf('.');
        
        if (index > 1) {
            // we are nested
            
            NormalizedFieldAndValue revisedField = new NormalizedFieldAndValue(origField);
            
            String baseFieldName = null;
            String group = null;
            String subgroup = null;
            
            baseFieldName = splits[0];
            group = originalIndexedFieldName.substring(index + 1);
            
            if (parentGroupingMatch.matcher(group).matches()) {
                group = trimGroup(group);
            } else {
                // not every field has a group
                if (splits.length == 2) {
                    subgroup = splits[1];
                } else if (splits.length >= 3) {
                    group = splits[1];
                    // last member is always the subgroup
                    subgroup = splits[splits.length - 1];
                }
                revisedField.setSubGroup(subgroup);
            }
            
            revisedField.setGrouped(true);
            revisedField.setGroup(group);
            revisedField.setIndexedFieldName(baseFieldName);
            
            return revisedField;
        } else {
            return origField;
        }
    }
    
    private String trimGroup(String groupStr) {
        int lastIndex = groupStr.lastIndexOf(".");
        if (lastIndex > 0) {
            groupStr = groupStr.substring(0, lastIndex);
        }
        // For fields where we previously needed to define configurations for all permutations of fields with parents P and their groups, (e.g.
        // FIELD.P1_n.P2_m...FIELD_x for all real integers n, m, and x) we can now simplify to P1_P2.
        groupStr = parentGroupingPattern.matcher(groupStr).replaceAll("");
        
        return groupStr;
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
