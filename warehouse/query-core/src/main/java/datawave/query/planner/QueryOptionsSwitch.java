package datawave.query.planner;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryOptions;
import datawave.util.StringUtils;

import java.util.Map;

public class QueryOptionsSwitch {
    
    public static void apply(Map<String,String> optionsMap, ShardQueryConfiguration config) {
        for (Map.Entry<String,String> entry : optionsMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            switch (key) {
                case QueryOptions.INCLUDE_GROUPING_CONTEXT:
                    config.setIncludeGroupingContext(Boolean.parseBoolean(value));
                    break;
                case QueryOptions.HIT_LIST:
                    config.setHitList(Boolean.parseBoolean(value));
                    break;
                case QueryOptions.LIMIT_FIELDS:
                    String[] lf = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setLimitFields(Sets.newHashSet(lf));
                    break;
                case QueryOptions.TYPE_METADATA_IN_HDFS:
                    config.setTypeMetadataInHdfs(Boolean.parseBoolean(value));
                    break;
                case QueryOptions.GROUP_FIELDS:
                    String[] groups = StringUtils.split(value, Constants.PARAM_VALUE_SEP);
                    config.setLimitFields(Sets.newHashSet(groups));
                    break;
            }
        }
    }
}
