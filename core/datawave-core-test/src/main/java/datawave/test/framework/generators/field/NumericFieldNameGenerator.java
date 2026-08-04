package datawave.test.framework.generators.field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A field name generator that produces numeric field names such as <code>1, 2, 3</code>
 */
public class NumericFieldNameGenerator implements FieldNameGenerator {

    private final List<String> fieldNames = new ArrayList<>();

    public static NumericFieldNameGenerator create() {
        return new NumericFieldNameGenerator();
    }

    private NumericFieldNameGenerator() {
        // no-op
    }

    /**
     * Generate N field names
     *
     * @param n
     *            the number of field names to generate
     */
    public void generate(int n) {
        for (int i = 1; i <= n; i++) {
            String field = String.valueOf(i);
            fieldNames.add(field + "_1");
        }
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<String> getRandomizedFieldNames() {
        List<String> randomized = new ArrayList<>(fieldNames);
        Collections.shuffle(randomized);
        return randomized;
    }

    @Override
    public boolean isNumeric() {
        return true;
    }
}
