package datawave.query.planner;

import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.UniqueFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.util.StringUtils;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

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
                case QueryParameters.MATCHING_FIELD_SETS:
                    String[] mfs = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setMatchingFieldSets(Sets.newHashSet(mfs));
                    break;
                case QueryParameters.GROUP_FIELDS:
                    String[] groups = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setGroupFields(Sets.newHashSet(groups));
                    config.setProjectFields(Sets.newHashSet(groups));
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
            }
        }
    }
}
