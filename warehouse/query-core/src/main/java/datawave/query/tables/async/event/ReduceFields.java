package datawave.query.tables.async.event;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.VariableNameVisitor;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility that reduces the set of fields prior to serialization
 */
public class ReduceFields {

    private ReduceFields() {
        // this is a static utility
    }

    /**
     * Get all query fields. Identifiers have the leading character stripped off.
     *
     * @param script
     *            the query script
     * @return a set of deconstructed fields
     */
    public static Set<String> getQueryFields(ASTJexlScript script) {
        Set<String> queryFields = new HashSet<>();
        for (String field : VariableNameVisitor.parseQuery(script)) {
            queryFields.add(JexlASTHelper.deconstructIdentifier(field));
        }
        return queryFields;
    }

    /**
     * Reduce the fields for an option
     *
     * @param key
     *            a {@link QueryOptions}
     * @param queryFields
     *            the set of fields in the query
     * @param settings
     *            the IteratorSetting
     */
    public static void reduceFieldsForOption(String key, Set<String> queryFields, IteratorSetting settings) {
        Map<String,String> options = settings.getOptions();
        if (options.containsKey(key)) {
            String reduced = intersectFields(queryFields, options.get(key));
            if (reduced != null) {
                // overwrite the option in the IteratorSetting
                settings.addOption(key, reduced);
            }
        }
    }

    /**
     * Intersect fields from the query and the option
     *
     * @param queryFields
     *            fields in the query
     * @param option
     *            a comma-delimited String of fields
     * @return the intersected fields as a comma-delimited String, or null if no such intersection exists
     */
    public static String intersectFields(Set<String> queryFields, String option) {
        Set<String> optionFields = new HashSet<>(Arrays.asList(org.apache.commons.lang3.StringUtils.split(option, ',')));
        optionFields = intersectFields(queryFields, optionFields);

        if (optionFields.isEmpty()) {
            return null;
        } else {
            return Joiner.on(',').join(optionFields);
        }
    }

    /**
     * Intersect the query fields with the option fields
     *
     * @param queryFields
     *            set of fields in the query
     * @param optionFields
     *            set of fields in the option
     * @return the intersected set of fields
     */
    public static Set<String> intersectFields(Set<String> queryFields, Set<String> optionFields) {
        return Sets.intersection(queryFields, optionFields);
    }
}
