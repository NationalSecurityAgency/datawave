package datawave.query.util;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.visitors.CaseSensitivityVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.model.QueryModel;

public class ShardQueryUtils {

    private static final Logger log = Logger.getLogger(ShardQueryUtils.class);

    /**
     * Uppercases all identifiers in the given script, as well as in the configuration's group-by fields, unique fields, excerpt fields, user projection fields,
     * disallow listed fields, and limit fields.
     *
     * @param metadataHelper
     *            the metadata helper
     * @param config
     *            the query configuration
     * @param script
     *            the query script
     * @return the updated query
     */
    public static ASTJexlScript upperCaseIdentifiers(MetadataHelper metadataHelper, ShardQueryConfiguration config, ASTJexlScript script) {
        GroupFields groupFields = config.getGroupFields();
        if (groupFields != null && groupFields.hasGroupByFields()) {
            groupFields.setMaxFields(toUpperCase(groupFields.getMaxFields()));
            groupFields.setSumFields(toUpperCase(groupFields.getSumFields()));
            groupFields.setGroupByFields(toUpperCase(groupFields.getGroupByFields()));
            groupFields.setAverageFields(toUpperCase(groupFields.getAverageFields()));
            groupFields.setCountFields(toUpperCase(groupFields.getCountFields()));
            groupFields.setMinFields(toUpperCase(groupFields.getMinFields()));

            // If grouping is set, we must make the projection fields match all the group-by fields and aggregation fields.
            config.setProjectFields(groupFields.getProjectionFields());
        } else {
            Set<String> projectFields = config.getProjectFields();

            if (projectFields != null && !projectFields.isEmpty()) {
                config.setProjectFields(toUpperCase(projectFields));
            }
        }

        UniqueFields uniqueFields = config.getUniqueFields();
        if (uniqueFields != null && !uniqueFields.isEmpty()) {
            Sets.newHashSet(uniqueFields.getFields()).stream().forEach(s -> uniqueFields.replace(s, s.toUpperCase()));
        }

        ExcerptFields excerptFields = config.getExcerptFields();
        if (excerptFields != null && !excerptFields.isEmpty()) {
            Sets.newHashSet(excerptFields.getFields()).stream().forEach(s -> excerptFields.replace(s, s.toUpperCase()));
        }

        Set<String> userProjection = config.getRenameFields();
        if (userProjection != null && !userProjection.isEmpty()) {
            config.setRenameFields(toUpperCase(userProjection));
        }

        Set<String> disallowlistedFields = config.getDisallowlistedFields();
        if (disallowlistedFields != null && !disallowlistedFields.isEmpty()) {
            config.setDisallowlistedFields(toUpperCase(disallowlistedFields));
        }

        Set<String> limitFields = config.getLimitFields();
        if (limitFields != null && !limitFields.isEmpty()) {
            config.setLimitFields(toUpperCase(limitFields));
        }

        return (CaseSensitivityVisitor.upperCaseIdentifiers(config, metadataHelper, script));
    }

    /**
     * Applies the query model to the given query script and query configuration. If cacheDataTypes is true and allFieldTypeMap is not null, allFieldTypeMap
     * will be used to fetch the set of all fields seen for the set of datatype filters within the config, and will be updated as needed. If log is not null,
     * messages documenting the field expansion changes will be logged at the trace level.
     *
     * @param script
     *            the query script
     * @param config
     *            the query configuration
     * @param allFields
     *            the set of all fields that exist
     * @param queryModel
     *            the query model
     * @return the updated query
     */
    public static ASTJexlScript applyQueryModel(ASTJexlScript script, ShardQueryConfiguration config, Set<String> allFields, QueryModel queryModel) {
        // Create the inverse of the reverse mapping: {display field name => db field name}
        // A reverse mapping is always many to on, therefore the inverted reverse mapping can be one to many.
        Multimap<String,String> inverseReverseModel = HashMultimap.create();
        queryModel.getReverseQueryMapping().entrySet().forEach(entry -> inverseReverseModel.put(entry.getValue(), entry.getKey()));
        inverseReverseModel.putAll(queryModel.getForwardQueryMapping());

        // Update the projection fields.
        Collection<String> projectFields = config.getProjectFields(), disallowlistedFields = config.getDisallowlistedFields(),
                        limitFields = config.getLimitFields();
        if (projectFields != null && !projectFields.isEmpty()) {
            projectFields = queryModel.remapParameter(projectFields, inverseReverseModel);
            if (log != null && log.isTraceEnabled()) {
                log.trace("Updated projection set using query model to: " + projectFields);
            }
            config.setProjectFields(Sets.newHashSet(projectFields));
        }

        // Update the group-by fields.
        GroupFields groupFields = config.getGroupFields();
        if (groupFields != null && groupFields.hasGroupByFields()) {
            groupFields.remapFields(inverseReverseModel, queryModel.getReverseQueryMapping());
            if (log.isTraceEnabled()) {
                log.trace("Updating group-by fields using query model to: " + groupFields);
            }
            config.setGroupFields(groupFields);

            // If grouping is set, we must make the projection fields match all the group-by fields and aggregation fields.
            config.setProjectFields(groupFields.getProjectionFields());
        }

        // Update the unique fields.
        UniqueFields uniqueFields = config.getUniqueFields();
        if (uniqueFields != null && !uniqueFields.isEmpty()) {
            uniqueFields.remapFields(inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated unique set using query model to: " + uniqueFields.getFields());
            }
            config.setUniqueFields(uniqueFields);
        }

        // Update the excerpt fields.
        ExcerptFields excerptFields = config.getExcerptFields();
        if (excerptFields != null && !excerptFields.isEmpty()) {
            excerptFields.expandFields(inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated excerpt fields using query model to: " + excerptFields.getFields());
            }
            config.setExcerptFields(excerptFields);
        }

        // Update the user projection fields.
        Set<String> userProjection = config.getRenameFields();
        if (userProjection != null && !userProjection.isEmpty()) {
            userProjection = Sets.newHashSet(queryModel.remapParameterEquation(userProjection, inverseReverseModel));
            if (log.isTraceEnabled()) {
                log.trace("Updated user projection fields using query model to: " + userProjection);
            }
            config.setRenameFields(userProjection);
        }

        // Update the disallow fields.
        if (config.getDisallowlistedFields() != null && !config.getDisallowlistedFields().isEmpty()) {
            disallowlistedFields = queryModel.remapParameter(disallowlistedFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated disallowlist set using query model to: " + disallowlistedFields);
            }
            config.setDisallowlistedFields(Sets.newHashSet(disallowlistedFields));
        }

        // Update the limit fields.
        if (config.getLimitFields() != null && !config.getLimitFields().isEmpty()) {
            limitFields = queryModel.remapParameterEquation(limitFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated limitFields set using query model to: " + limitFields);
            }
            config.setLimitFields(Sets.newHashSet(limitFields));
        }

        return (QueryModelVisitor.applyModel(script, queryModel, allFields, config.getNoExpansionFields(), config.getLenientFields(),
                        config.getStrictFields()));
    }

    /**
     * Returns a copy of the given set with all strings uppercased.
     *
     * @param strs
     *            the strings
     * @return the uppercased strings
     */
    private static Set<String> toUpperCase(Collection<String> strs) {
        return strs.stream().map(String::toUpperCase).collect(Collectors.toSet());
    }

    /**
     * Do not allow this class to be instatiated.
     */
    private ShardQueryUtils() {
        throw new UnsupportedOperationException();
    }
}
