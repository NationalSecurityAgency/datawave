package datawave.table.constants;

public final class QueryConstants {

    /**
     * The null byte character.
     */
    public static final char NULL_BYTE_CHAR = '\u0000';

    /**
     * The value of {@link #NULL_BYTE_CHAR} as a single byte, i.e. {@code 0x00}.
     */
    public static final byte NULL_BYTE = (byte) NULL_BYTE_CHAR;

    /**
     * The value of {@link #NULL_BYTE_CHAR} as a string.
     */
    public static final String NULL_BYTE_STR = Character.toString(NULL_BYTE_CHAR);

    /**
     * The value of {@link Character#MAX_CODE_POINT} as a string.
     */
    public static final String MAX_UNICODE_STR = Character.toString(Character.MAX_CODE_POINT);

    /**
     * Enforce static access.
     */
    private QueryConstants() {
        throw new UnsupportedOperationException();
    }
}
