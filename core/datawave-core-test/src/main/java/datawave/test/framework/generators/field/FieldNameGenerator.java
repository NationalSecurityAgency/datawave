package datawave.test.framework.generators.field;

import java.util.List;

public interface FieldNameGenerator {

    /**
     * Generate N field names
     *
     * @param n
     *            the number of field names to generate
     */
    void generate(int n);

    /**
     * Get the field names that were generated
     *
     * @return the list of field names
     */
    List<String> getFieldNames();

    /**
     * Get the field names in random order
     *
     * @return the list of field names in random order
     */
    List<String> getRandomizedFieldNames();

    /**
     * Flag that indicates if this field is a numeric
     *
     * @return true if the field name is a number
     */
    boolean isNumeric();
}
