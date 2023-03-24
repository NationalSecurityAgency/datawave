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
    private static final String DOT = ".";
    
    public SimpleGroupFieldNameParser() {}
    
    /**
     * @param origField
     *            the NormalizedContentInterface potentially containing a composite field name
     * @return a NormalizedContentInterface that contains a modified indexedFieldName and group, extracted from the original indexed field name
     */
    public NormalizedContentInterface extractFieldNameComponents(NormalizedContentInterface origField) {
        String originalIndexedFieldName = origField.getIndexedFieldName();
        String[] splits = StringUtils.split(originalIndexedFieldName, '.');
        
        if (splits.length > 1) {
            // we are nested, i.e. we have a field parent names in the event field name
            
            NormalizedFieldAndValue revisedField = new NormalizedFieldAndValue(origField);
            
            String baseFieldName = null;
            String origGroup = null;
            String group = null;
            String subgroup = null;
            
            baseFieldName = splits[0];
            origGroup = extractGroup(splits);
            
            group = trimGroup(origGroup);
            
            if (group.equals(origGroup)) {
                // we've not been trimmed. we don't have parent subgroups
                group = null;
                
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
    
    // strips the first and last element of the fieldname to extract the group
    private String extractGroup(String[] splits) {
        StringBuilder group = new StringBuilder();
        group.append(splits[1]);
        
        for (int i = 2; i < splits.length - 1; i++) {
            group.append(DOT);
            group.append(splits[i]);
        }
        return group.toString();
    }
    
    // For fields where we previously needed to define configurations for all permutations of fields with parents P and their groups, (e.g.
    // FIELD.P1_n.P2_m...FIELD_x for all real integers n, m, and x) we can now simplify to P1_P2.
    public static String trimGroup(String groupStr) {
        StringBuilder group = new StringBuilder();
        boolean checkForSubgroup = false;
        
        int groupStart = -1;
        for (int i = 0; i < groupStr.length(); i++) {
            char c = groupStr.charAt(i);
            if (checkForSubgroup) {
                if (c == '.') {
                    group.append(c);
                    checkForSubgroup = false;
                } else if (Character.isDigit(c)) {
                    // continue
                } else if (c == '_') {
                    // might be entering another subgroup candidate
                    group.append(groupStr.substring(groupStart, i));
                    groupStart = i;
                } else {
                    group.append(groupStr.substring(groupStart, i + 1));
                    checkForSubgroup = false;
                }
            } else if (c == '_') {
                checkForSubgroup = true;
                groupStart = i;
            } else {
                group.append(c);
            }
        }
        
        return group.toString();
        
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
