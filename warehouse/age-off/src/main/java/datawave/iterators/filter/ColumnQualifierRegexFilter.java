/**
 *
 */
package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This subclass of the {@code RegexFilterBase} class is used to filter based on the column qualifier of the {@code Key} object {@code k}.
 */
public class ColumnQualifierRegexFilter extends RegexFilterBase {
    /**
     * This method returns a {@code String} object containing the column qualifier for {@code Key} object {@code k}
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code String} object containing the column qualifier of {@code k}
     */
    @Override
    protected String getKeyField(Key k, Value v) {
        return k.getColumnQualifier().toString();
    }

}
