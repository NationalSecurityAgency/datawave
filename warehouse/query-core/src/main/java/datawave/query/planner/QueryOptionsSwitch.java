package datawave.query.planner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.SummaryOptions;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.util.StringUtils;

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
                    String[] lf = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setLimitFields(Sets.newHashSet(lf));
                    break;
                case QueryParameters.MATCHING_FIELD_SETS:
                    String[] mfs = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setMatchingFieldSets(Sets.newHashSet(mfs));
                    break;
                case QueryParameters.GROUP_FIELDS:
                    String[] groups = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    groupFields = config.getGroupFields();
                    groupFields.setGroupByFields(Sets.newHashSet(groups));
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
                    ExcerptFields excerptFields = ExcerptFields.from(value);
                    config.setExcerptFields(excerptFields);
                    break;
                case QueryParameters.SUMMARY_OPTIONS:
                    SummaryOptions summaryOptions = SummaryOptions.from(value);
                    config.setSummaryOptions(summaryOptions);
                    break;
                case QueryParameters.NO_EXPANSION_FIELDS:
                    config.setNoExpansionFields(new HashSet<>(Arrays.asList(StringUtils.split(value, Constants.PARAM_VALUE_SEP))));
                    break;
                case QueryParameters.LENIENT_FIELDS:
                    config.setLenientFields(new HashSet<>(Arrays.asList(StringUtils.split(value, Constants.PARAM_VALUE_SEP))));
                    break;
                case QueryParameters.STRICT_FIELDS:
                    config.setStrictFields(new HashSet<>(Arrays.asList(StringUtils.split(value, Constants.PARAM_VALUE_SEP))));
                    break;
                case QueryParameters.RENAME_FIELDS:
                    Set<String> renameFieldExpressions = new HashSet<>(Arrays.asList(StringUtils.split(value, Constants.PARAM_VALUE_SEP)));
                    config.setRenameFields(renameFieldExpressions);
                    break;
                case QueryParameters.SUM_FIELDS:
                    String[] sumFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    groupFields = config.getGroupFields();
                    groupFields.setSumFields(Sets.newHashSet(sumFields));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.MAX_FIELDS:
                    String[] maxFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    groupFields = config.getGroupFields();
                    groupFields.setMaxFields(Sets.newHashSet(maxFields));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.MIN_FIELDS:
                    String[] minFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    groupFields = config.getGroupFields();
                    groupFields.setMinFields(Sets.newHashSet(minFields));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.COUNT_FIELDS:
                    String[] countFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    groupFields = config.getGroupFields();
                    groupFields.setCountFields(Sets.newHashSet(countFields));
                    config.setGroupFields(groupFields);
                    // Update the projection fields only if we have group-by fields specified.
                    if (groupFields.hasGroupByFields()) {
                        config.setProjectFields(groupFields.getProjectionFields());
                    }
                    break;
                case QueryParameters.AVERAGE_FIELDS:
                    String[] averageFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    groupFields = config.getGroupFields();
                    groupFields.setAverageFields(Sets.newHashSet(averageFields));
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
