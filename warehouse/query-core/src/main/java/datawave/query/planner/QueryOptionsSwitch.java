package datawave.query.planner;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.config.ShardQueryConfiguration;
import datawave.util.StringUtils;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.log4j.Logger;

import java.util.Map;

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
                    String[] uniqueFields = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setUniqueFields(Sets.newHashSet(uniqueFields));
            }
        }
    }
}
