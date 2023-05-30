package datawave.query.planner;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.attributes.ExcerptFields;
import datawave.query.common.grouping.GroupAggregateFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.attributes.UniqueFields;
import datawave.util.StringUtils;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QueryOptionsSwitch {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryOptionsSwitch.class);
    
    public static void apply(Map<String,String> optionsMap, ShardQueryConfiguration config) {
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
                case QueryParameters.GROUP_FIELDS:
                    String[] groups = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setGroupFields(Sets.newHashSet(groups));
                    // Update the projection fields.
                    addToProjectionFields(config, groups);
                    // Update the group aggregate fields.
                    getOrCreateGroupAggregateFields(config).addGroupFields(groups);
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
                    config.setUniqueFields(uniqueFields);
                    break;
                case QueryParameters.EXCERPT_FIELDS:
                    ExcerptFields excerptFields = ExcerptFields.from(value);
                    config.setExcerptFields(excerptFields);
                    break;
                case QueryParameters.SUM_FIELDS:
                    String[] sumFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    // Update the projection fields.
                    addToProjectionFields(config, sumFields);
                    // Update the group aggregate fields.
                    getOrCreateGroupAggregateFields(config).addSumFields(sumFields);
                    break;
                case QueryParameters.MAX_FIELDS:
                    String[] maxFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    // Update the projection fields.
                    addToProjectionFields(config, maxFields);
                    // Update the group aggregate fields.
                    getOrCreateGroupAggregateFields(config).addMaxFields(maxFields);
                    break;
                case QueryParameters.MIN_FIELDS:
                    String[] minFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    // Update the projection fields.
                    addToProjectionFields(config, minFields);
                    // Update the group aggregate fields.
                    getOrCreateGroupAggregateFields(config).addMinFields(minFields);
                    break;
                case QueryParameters.COUNT_FIELDS:
                    String[] countFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    // Update the projection fields.
                    addToProjectionFields(config, countFields);
                    // Update the group aggregate fields.
                    getOrCreateGroupAggregateFields(config).addCountFields(countFields);
                    break;
                case QueryParameters.AVERAGE_FIELDS:
                    String[] averageFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    // Update the projection fields.
                    addToProjectionFields(config, averageFields);
                    // Update the group aggregate fields.
                    getOrCreateGroupAggregateFields(config).addAverageFields(averageFields);
                    break;
            }
        }
    }
    
    public static void addToProjectionFields(ShardQueryConfiguration config, String[] fields) {
        Set<String> projectFields = config.getProjectFields();
        if (projectFields == null) {
            projectFields = new HashSet<>();
        }
        projectFields.addAll(Arrays.asList(fields));
        config.setProjectFields(projectFields);
    }
    
    public static GroupAggregateFields getOrCreateGroupAggregateFields(ShardQueryConfiguration config) {
        GroupAggregateFields groupAggregateFields = config.getGroupAggregateFields();
        if (groupAggregateFields == null) {
            groupAggregateFields = new GroupAggregateFields();
            config.setGroupAggregateFields(groupAggregateFields);
        }
        return groupAggregateFields;
    }
}
