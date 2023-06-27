package datawave.query.data.parsers;

/**
 * Utility to create a {@link KeyParser} from a {@link PARSER_TYPE}
 */
public class KeyParserFactory {

    public enum PARSER_TYPE {
        FIELD_INDEX, EVENT, TERM_FREQUENCY
    }

    private KeyParserFactory() {
        // static utility
    }

    /**
     * Create a KeyParser from the provided type
     *
     * @param type
     *            the kind of key parser to create
     * @return a key parser
     */
    public static KeyParser create(PARSER_TYPE type) {
        switch (type) {
            case FIELD_INDEX:
                return new FieldIndexKey();
            case EVENT:
                return new EventKey();
            case TERM_FREQUENCY:
                return new TermFrequencyKey();
            default:
                return null;
        }
    }
}
