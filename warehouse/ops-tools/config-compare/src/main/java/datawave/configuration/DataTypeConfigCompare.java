package datawave.configuration;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * A utility for comparing data type configuration files. The comparison will report fields that are the same and fields that are different.
 * <p>
 * The comparison follows the following rules: 1) There must be a data.name config. This will be used as the expected prefix. <br>
 * 2) If a field starts with the detected prefix, it will be compared to the corresponding (prefixed) value in the other config. <br>
 * 3) If a field is not prefixed, it will be compared to the same field in the other config. <br>
 */
public class DataTypeConfigCompare {

    public static final String PREFIX = "data.name";

    /**
     * Runs the comparison.
     *
     * @param left
     *            left-hand Configuration
     * @param right
     *            right-hand Configuration
     * @return CompareResult which houses comparison details.
     */
    public CompareResult run(Configuration left, Configuration right) {
        SortedSet<String> same = new TreeSet<>();
        SortedSet<String> diff = new TreeSet<>();
        SortedSet<String> leftOnly = new TreeSet<>();
        SortedSet<String> rightOnly = new TreeSet<>();

        String leftPrefix = getPrefix(left);
        String rightPrefix = getPrefix(right);

        for (Map.Entry<String,String> entry : left) {
            ConfField field = new ConfField(leftPrefix, entry.getKey());

            String leftValue = entry.getValue();
            String rightValue = right.get(field.getField(rightPrefix));

            if (nullSafeEquals(leftValue, rightValue)) {
                same.add(field.getField());
            } else if (rightValue == null) {
                leftOnly.add(field.getField());
            } else {
                diff.add(field.getField());
            }
        }

        // To find values only in right, we just iterate through
        // and verify each property does not exist in left, since
        // we already checked equivalence above.
        for (Map.Entry<String,String> entry : right) {
            ConfField field = new ConfField(rightPrefix, entry.getKey());
            if (left.get(field.getField(leftPrefix)) == null) {
                rightOnly.add(field.getField());
            }
        }

        return new CompareResult(same, diff, leftOnly, rightOnly);
    }

    private boolean nullSafeEquals(String s1, String s2) {
        return s1 == null ? s2 == null : s1.equals(s2);
    }

    private String getPrefix(Configuration c) {
        String prefix = c.get(PREFIX);

        if (StringUtils.isBlank(prefix)) {
            throw new IllegalArgumentException("Configurations must contain a 'data.name' field.");
        }

        return prefix;
    }
}
