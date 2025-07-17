package datawave.query.function;

import javax.annotation.Nullable;

/**
 * Parser provides capability to convert a field into a commonality key and grouping token.
 */
public class CommonalityAndGroupParser {
    /**
     * Decodes a field into a commonality key and grouping token, if possible. In the case where the field does not contain a commonality key and grouping token
     * a null is returned.
     *
     * @param field
     *            the field to attempt to convert
     * @return the commonality and grouping token, otherwise null if there is none
     */
    @Nullable
    public CommonalityAndGroup parse(String field) {
        CommonalityAndGroup result = null;
        int keyOffset = -1;
        int keyEndOffset = -1;
        int groupOffset = -1;
        int totalTokens = 0;

        int i = 0;

        while ((i = field.indexOf('.', i)) != -1) {
            if (totalTokens == 0) {
                keyOffset = i + 1;
            } else if (totalTokens == 1) {
                keyEndOffset = i;
            }
            if (totalTokens >= 1) {
                // the group offset is after a delimiter is found (or the last sequence of characters
                // after the delimiter is found)
                groupOffset = i + 1;
            }
            totalTokens++;
            i++;
        }

        // Return key/group for the following examples:
        // FIELD_3.FIELD.5
        // FIELD_3.FIELD.4.5
        if (totalTokens >= 2 && groupOffset != field.length()) {
            String keyVal = field.substring(keyOffset, keyEndOffset);
            String groupVal = field.substring(groupOffset, field.length() - groupOffset + groupOffset);
            return new CommonalityAndGroup(keyVal, groupVal);
        }

        return result;
    }

    public String removeGrouping(String key) {
        // if we have grouping context on, remove the grouping context
        int index = key.indexOf('.');
        if (index != -1) {
            key = key.substring(0, index);
        }
        return key;
    }
}
