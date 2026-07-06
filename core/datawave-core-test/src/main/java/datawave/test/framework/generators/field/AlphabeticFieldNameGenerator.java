package datawave.test.framework.generators.field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A field name generator that produces alphabetic field names such as <code>A, B, C</code>
 */
public class AlphabeticFieldNameGenerator implements FieldNameGenerator {

    private static final int ALPHABET_SIZE = 26;

    private final List<String> fieldNames = new ArrayList<>();

    public static AlphabeticFieldNameGenerator create() {
        return new AlphabeticFieldNameGenerator();
    }

    private AlphabeticFieldNameGenerator() {
        // no-op
    }

    public void generate(int n) {
        int start = fieldNames.size() + 1;
        for (int i = start; i < start + n; i++) {
            fieldNames.add(toFieldName(i));
        }
    }

    /**
     * Converts a 1-based index into a spreadsheet-style alphabetic name, e.g. 1 -&gt; A, 26 -&gt; Z, 27 -&gt; AA, 52 -&gt; AZ, 53 -&gt; BA, 702 -&gt; ZZ, 703
     * -&gt; AAA
     *
     * @param index
     *            a 1-based index
     * @return the alphabetic field name for the index
     */
    private static String toFieldName(int index) {
        StringBuilder name = new StringBuilder();
        int remaining = index;
        while (remaining > 0) {
            remaining--;
            int letterIndex = remaining % ALPHABET_SIZE;
            name.insert(0, (char) ('A' + letterIndex));
            remaining /= ALPHABET_SIZE;
        }
        return name.toString();
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
        return false;
    }
}
