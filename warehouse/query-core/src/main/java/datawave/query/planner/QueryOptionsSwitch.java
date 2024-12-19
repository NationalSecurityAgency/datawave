package datawave.query.planner;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.SummaryOptions;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.config.ShardQueryConfiguration;

public class QueryOptionsSwitch {

    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryOptionsSwitch.class);

    public static void apply(Map<String,String> optionsMap, ShardQueryConfiguration config) {
        GroupFields groupFields;
        for (Map.Entry<String,String> entry : optionsMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            switch (key) {
                case QueryParameters.INCLUDE_GROUPING_CONTEXT:
                    config.setIncludeGroupingContext(Boolean.parseBoolean(value));
                    break;
                case QueryParameters.HIT_LIST:
                    config.setHitList(Boolean.parseBoolean(value));
                    break;
                case QueryParameters.LIMIT_FIELDS:
                    config.setLimitFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    break;
                case QueryParameters.MATCHING_FIELD_SETS:
                    config.setMatchingFieldSets(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    break;
                case QueryParameters.GROUP_FIELDS:
                    groupFields = config.getGroupFields();
                    groupFields.setGroupByFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    config.setGroupFields(groupFields);
                    // If there are any group-by fields, update the projection fields to include them.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.GROUP_FIELDS_BATCH_SIZE:
                    try {
                        config.setGroupFieldsBatchSize(Integer.parseInt(value));
                    } catch (Exception ex) {
                        log.warn("Could not parse " + value + " as group.fields.batch.size");
                    }
                    break;
                case QueryParameters.UNIQUE_FIELDS:
                    UniqueFields uniqueFields = UniqueFields.from(value);
                    // preserve the most recent flag
                    uniqueFields.setMostRecent(config.getUniqueFields().isMostRecent());
                    config.setUniqueFields(uniqueFields);
                    break;
                case QueryParameters.MOST_RECENT_UNIQUE:
                    log.info("Setting unique fields to be most recent");
                    config.getUniqueFields().setMostRecent(Boolean.parseBoolean(value));
                    break;
                case QueryParameters.EXCERPT_FIELDS:
                    config.setExcerptFields(ExcerptFields.from(value));
                    break;
                case QueryParameters.SUMMARY_OPTIONS:
                    config.setSummaryOptions(SummaryOptions.from(value));
                    break;
                case QueryParameters.NO_EXPANSION_FIELDS:
                    config.setNoExpansionFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    break;
                case QueryParameters.LENIENT_FIELDS:
                    config.setLenientFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    break;
                case QueryParameters.STRICT_FIELDS:
                    config.setStrictFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    break;
                case QueryParameters.RENAME_FIELDS:
                    config.setRenameFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    break;
                case QueryParameters.SUM_FIELDS:
                    groupFields = config.getGroupFields();
                    groupFields.setSumFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.MAX_FIELDS:
                    groupFields = config.getGroupFields();
                    groupFields.setMaxFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.MIN_FIELDS:
                    groupFields = config.getGroupFields();
                    groupFields.setMinFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.COUNT_FIELDS:
                    groupFields = config.getGroupFields();
                    groupFields.setCountFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.AVERAGE_FIELDS:
                    groupFields = config.getGroupFields();
                    groupFields.setAverageFields(new HashSet<>(List.of(value.split(Constants.COMMA))));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
            }
        }
    }
}
