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
        String[] splits = field.split("\\.");
        if (splits.length >= 3) {
            // return the first group and last group (a.k.a the instance in the first group)
            return new CommonalityAndGroup(splits[1], splits[splits.length - 1]);
        }
        return null;

    }
}
