package datawave.ingest.data.normalizer;

import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

/**
 * Consolidated logic from SimpleGroupNameNormalizer and others.
 */
public class SimpleGroupFieldNameParser {
    private static final long serialVersionUID = 1035918631638323565L;
    private static final Logger log = Logger.getLogger(SimpleGroupFieldNameParser.class);
    private static final char DOT = '.';

    public SimpleGroupFieldNameParser() {}

    /**
     * @param origField
     *            the NormalizedContentInterface potentially containing a composite field name
     * @return a NormalizedContentInterface that contains a modified indexedFieldName and group, extracted from the original indexed field name
     */
    public NormalizedContentInterface extractFieldNameComponents(NormalizedContentInterface origField) {
        String originalIndexedFieldName = origField.getIndexedFieldName();
        int index = originalIndexedFieldName.indexOf(DOT);

        if (index > -1) {
            // this field name has a group component
            String baseFieldName = originalIndexedFieldName.substring(0, index);
            String group = originalIndexedFieldName.substring(index + 1);

            NormalizedFieldAndValue revisedField = new NormalizedFieldAndValue(origField);

            revisedField.setGrouped(true);
            revisedField.setGroup(group);
            revisedField.setIndexedFieldName(baseFieldName);

            return revisedField;
        } else {
            return origField;
        }
    }

    /**
     * @param origField
     *            the NormalizedContentInterface potentially containing a composite field name
     * @return a NormalizedContentInterface that contains a modified indexedFieldName and group, extracted from the original indexed field name with parent
     *         offsets trimmed
     */
    public NormalizedContentInterface extractAndTrimFieldNameComponents(NormalizedContentInterface origField) {
        String originalIndexedFieldName = origField.getIndexedFieldName();
        String[] components = extractTrimmedGroupAndSubGroup(originalIndexedFieldName);

        String baseFieldName = components[0];
        String group = components[1];
        String subgroup = components[2];

        if (null != group || null != subgroup) {

            NormalizedFieldAndValue revisedField = new NormalizedFieldAndValue(origField);

            revisedField.setGrouped(true);
            revisedField.setGroup(group);
            revisedField.setSubGroup(subgroup);
            revisedField.setIndexedFieldName(baseFieldName);

            return revisedField;
        } else {
            return origField;
        }
    }

    /**
     *
     * @param fullFieldName
     *            the complete event field name of the form FIELD_NAME.GROUP.{IntermediateGroups}.{Context or Field}
     * @return the component array. base field name : FIELD_NAME, groups: GROUP.{IntermediateGroups} with offsets removed, and subgroup {Context}
     */
    public String[] extractTrimmedGroupAndSubGroup(String fullFieldName) {
        String[] splits = StringUtils.split(fullFieldName, DOT);
        String baseFieldName = splits[0];
        String group = null;
        String subgroup = null;

        if (splits.length > 1) {
            // we are nested, i.e. we have a field parent names in the event field name
            String origGroup = extractGroup(splits);

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
            }

        }

        return new String[] {baseFieldName, group, subgroup};
    }

    /**
     *
     * @param origGroup
     *            the group and context GROUP.{IntermediateGroups}.{Context or Field}
     * @return GROUP.{IntermediateGroups} with parent offsets removed
     */
    public String getTrimmedGroup(String origGroup) {
        String[] splits = StringUtils.split(origGroup, DOT);

        origGroup = removeLastGroup(splits);
        String group = trimGroup(origGroup);

        if (group.equals(origGroup)) {
            // we've not been trimmed. we don't have parent subgroups
            group = null;

            // not every field has a group
            if (splits.length == 1) {
                group = null;
            } else if (splits.length >= 2) {
                group = splits[0];

            }
        }
        return group;
    }

    // strips the first and last element of the full fieldname to extract the group
    private String extractGroup(String[] splits) {
        StringBuilder group = new StringBuilder();
        group.append(splits[1]);

        for (int i = 2; i < splits.length - 1; i++) {
            group.append(DOT);
            group.append(splits[i]);
        }
        return group.toString();
    }

    // removes the last group
    private String removeLastGroup(String[] splits) {
        StringBuilder group = new StringBuilder();
        group.append(splits[0]);

        for (int i = 1; i < splits.length - 1; i++) {
            group.append(DOT);
            group.append(splits[i]);
        }
        return group.toString();
    }

    // For fields where we previously needed to define configurations for all permutations of fields with parents P and their groups, (e.g.
    // FIELD.P1_n.P2_m...FIELD_x for all real integers n, m, and x) we can now simplify to P1_P2.
    public String trimGroup(String groupStr) {
        StringBuilder group = new StringBuilder();
        boolean checkForSubgroup = false;

        int groupStart = -1;
        for (int i = 0; i < groupStr.length(); i++) {
            char c = groupStr.charAt(i);
            if (checkForSubgroup) {
                if (c == DOT) {
                    group.append(c);
                    checkForSubgroup = false;
                } else if (Character.isDigit(c)) {
                    // continue
                } else if (c == '_') {
                    // might be entering another subgroup candidate
                    group.append(groupStr, groupStart, i);
                    groupStart = i;
                } else {
                    group.append(groupStr, groupStart, i + 1);
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
