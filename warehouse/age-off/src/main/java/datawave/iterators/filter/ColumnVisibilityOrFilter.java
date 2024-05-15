/**
 *
 */
package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

@Deprecated
public class ColumnVisibilityOrFilter extends TokenFilterBase {

    /**
     * Determine if the key's column visibility matches the regex defined by {@link AgeOffConfigParams#MATCHPATTERN}
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return true if the provided key's column visibility matches the configured {@link TokenFilterBase}'s pattern
     */
    @Override
    public boolean hasToken(Key k, Value v, byte[][] testTokens) {
        boolean found = false;

        byte[] cv = k.getColumnVisibilityData().getBackingArray();

        // find the start of the first token to test
        int start = findNextNonDelimiter(cv, 0);
        int end;

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
