package datawave.ingest.mapreduce.handler.summary.util;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import datawave.ingest.data.config.NormalizedContentInterface;

public final class MetricRuleUtil {

    private MetricRuleUtil() {}

    private static final Matcher validRule = Pattern.compile("[a-z_0-9]{3,30}").matcher("");
    private static final Matcher longNumericSequence = Pattern.compile(".*[\\d]{5,}.*").matcher("");
    public static final String MISSING = "MISSING";

    public static boolean isValidRule(String eventFieldValue) {
        validRule.reset(eventFieldValue);
        longNumericSequence.reset(eventFieldValue);
        boolean matchesValidRuleFormat = validRule.matches();
        boolean notLongSequenceOfDigits = !longNumericSequence.matches();
        return matchesValidRuleFormat && notLongSequenceOfDigits;
    }

    public static String normalizeMetricRules(Collection<NormalizedContentInterface> fieldValues) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            return MISSING;
        }
        SortedSet<String> sortedUniqueRules = new TreeSet<>();
        for (NormalizedContentInterface fieldValue : fieldValues) {
            String eventFieldValue = fieldValue.getEventFieldValue();
            if (!isValidRule(eventFieldValue)) {
                continue;
            }
            if (StringUtils.isBlank(eventFieldValue)) {
                sortedUniqueRules.add(MISSING);
            } else {
                sortedUniqueRules.add(eventFieldValue);
            }
        }
        if (sortedUniqueRules.isEmpty()) {
            return MISSING;
        }
        return StringUtils.join(sortedUniqueRules, ",");
    }

}
