package datawave.metrics.analytic;

import datawave.metrics.config.MetricsConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public final class HourlyPrecisionHelper {
    private HourlyPrecisionHelper() {
        // help prevent instantiation
    }

    public static boolean checkForHourlyPrecisionOption(Configuration conf, Logger log) {
        String useHourlyPrecisionString = conf.get(MetricsConfig.USE_HOURLY_PRECISION);
        if (!StringUtils.isEmpty(useHourlyPrecisionString)) {
            log.debug("Using hourly precision");
            return Boolean.valueOf(useHourlyPrecisionString);
        }
        return false;
    }

    public static String getOutputTable(Configuration conf, boolean useHourlyPrecision) {
        if (useHourlyPrecision) {
            return conf.get(MetricsConfig.METRICS_HOURLY_SUMMARY_TABLE, MetricsConfig.DEFAULT_HOURLY_METRICS_SUMMARY_TABLE);
        } else {
            return conf.get(MetricsConfig.METRICS_SUMMARY_TABLE, MetricsConfig.DEFAULT_METRICS_SUMMARY_TABLE);
        }
    }
}
