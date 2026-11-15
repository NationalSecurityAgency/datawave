package datawave.table.constants;

public final class QueryConstants {

    public static final String NULL = "\u0000";

    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));

    private QueryConstants() {
        throw new UnsupportedOperationException();
    }
}
