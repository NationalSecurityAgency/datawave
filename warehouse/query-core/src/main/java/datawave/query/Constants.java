package datawave.query;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import datawave.query.jexl.functions.ContentFunctions;

public class Constants {
    public static final String NULL = "\u0000";
    public static final String NULL_BYTE_STRING = "\u0000";
    public static final Value NULL_VALUE = new Value(new byte[0]);

    public static final String JAVA_NULL_STRING = "null";

    public static final String ONE_BYTE = "\u0001";

    public static final String FIELD_INDEX_PREFIX = "fi" + NULL;

    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));

    public static final String SPACE = " ";

    public static final String COLON = ":";

    public static final String COMMA = ",";

    public static final String BRACKET_START = "[";

    public static final String BRACKET_END = "]";

    public static final String EQUALS = "=";

    public static final String FORWARD_SLASH = "/";

    public static final String LEFT_PAREN = "(";

    public static final String RIGHT_PAREN = ")";

    public static final String PIPE = "|";

    public static final String ASTERISK = "*";

    public static final String QUOTE = "\"";

    /**
     * The UTF-16 representation of the opening smart quote ASCII-147.
     */
    public static final String UTF_16_SMART_QUOTE_LEFT = "\\u0093";

    /**
     * The UTF-16 representation of the ending smart quote ASCII-148.
     */
    public static final String UTF_16_SMART_QUOTE_RIGHT = "\\u0094";

    /**
     * The UTF-16 representation of the opening double quote.
     */
    public static final String UTF_16_DOUBLE_QUOTE_LEFT = "\\u201c";

    /**
     * The UTF-16 representation of the ending double quote.
     */
    public static final String UTF_16_DOUBLE_QUOTE_RIGHT = "\\u201d";

    public static final Text TEXT_NULL = new Text(NULL);

    public static final Text FI_PREFIX = new Text("fi");

    public static final Text FI_PREFIX_WITH_NULL = new Text("fi\u0000");

    public static final String FI_PREFIX_WITH_NULL_STRING = "fi" + NULL_BYTE_STRING;

    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final String EMPTY_STRING = "";

    public static final Value EMPTY_VALUE = new Value(EMPTY_BYTES);

    public static final String INDEXED_TERMS_LIST = "INDEXED_TERMS_LIST";

    public static final String ANY_FIELD = "_ANYFIELD_";

    public static final String NO_FIELD = "_NOFIELD_";

    public static final Authorizations EMPTY_AUTHS = new Authorizations();

    public static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();

    // FieldIndex Query logic params
    public static final String UNIQ_DATATYPE = "unique.datatype";
    public static final String UNIQ_VISIBILITY = "unique.visibility";

    public static final String PARENT_UID = "PARENT_UID";
    public static final String CHILD_COUNT = "CHILD_COUNT";

    // From QueryOptions
    public static final String RETURN_TYPE = "return.type";

    // From RefactoredShardQueryConfig
    public static final char PARAM_VALUE_SEP = ',';

    // From ingest
    public static final Text TERM_FREQUENCY_COLUMN_FAMILY = new Text("tf");

    public static final Text D_COLUMN_FAMILY = new Text("d");

    // content functions
    public static final String TERM_OFFSET_MAP_JEXL_VARIABLE_NAME = ContentFunctions.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME;
    public static final String CONTENT_FUNCTION_NAMESPACE = ContentFunctions.CONTENT_FUNCTION_NAMESPACE;
    public static final String CONTENT_WITHIN_FUNCTION_NAME = ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME;
    public static final String CONTENT_ADJACENT_FUNCTION_NAME = ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME;
    public static final String CONTENT_PHRASE_FUNCTION_NAME = ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME;
    public static final String CONTENT_SCORED_PHRASE_FUNCTION_NAME = ContentFunctions.CONTENT_SCORED_PHRASE_FUNCTION_NAME;

    // RangeStream, temporary move
    public static final String SHARD_DAY_HINT = "SHARDS_AND_DAYS";

    // From evaluating iterator
    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";

    public static final String COLUMN_VISIBILITY = "columnVisibility";

    public static final Character BACKSLASH_CHAR = '\\';
    public static final Character ASTERISK_CHAR = '*';

    public static final String JEXL = "JEXL";
    public static final String LUCENE = "LUCENE";
    public static final String LUCENE_UUID = "LUCENE-UUID";
}
