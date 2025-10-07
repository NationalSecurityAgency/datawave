/**
 *
 */
package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This subclass of the {@code RegexFilterBase} class is used to filter based on the column visibility of the {@code Key} object {@code k}.
 */
public class ColumnVisibilityOrFilter extends TokenFilterBase {

    /**
     * This method returns a {@code Iterator<String>} iterator of tokens containing the column column visibility parts for {@code Key} object {@code k}
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code String} object containing the column visibility of {@code k}
     */
    @Override
    public boolean hasToken(Key k, Value v, byte[][] testTokens) {
        boolean found = false;

        byte[] cv = k.getColumnVisibilityData().getBackingArray();

        int start = -1;
        int end = -1;

        // find the start of the first token to test
        start = findNextNonDelimiter(cv, end + 1);

        // while not past the end and our test token was not found
        while (start < cv.length && !found) {
            // if the current position starts with our test token
            for (int i = 0; !found && i < testTokens.length; i++) {
                byte[] testToken = testTokens[i];
                if (startsWith(cv, start, testToken)) {
                    // ensure that the character past the test token is the end of string or a delimiter
                    end = start + testToken.length;
                    if (end == cv.length || isDelimiter(cv[end])) {
                        // success, we have an exact match!
                        found = true;
                    }
                }
            }
            if (!found) {
                // otherwise find the current token's end
                end = findNextDelimiter(cv, start + 1);
                // now find the start of the next token to test
                start = findNextNonDelimiter(cv, end + 1);
            }
        }

        return found;
    }

}
