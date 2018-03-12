package datawave.query;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

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
    
    public static final Text TEXT_NULL = new Text(NULL);
    
    public static final Text FI_PREFIX = new Text("fi");
    
    public static final Text FI_PREFIX_WITH_NULL = new Text("fi\u0000");
    
    public static final String FI_PREFIX_WITH_NULL_STRING = "fi" + NULL_BYTE_STRING;
    
    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final String EMPTY_STRING = "";
    
    public static final Value EMPTY_VALUE = new Value(EMPTY_BYTES);
    
    public static final String INDEXED_TERMS_LIST = "INDEXED_TERMS_LIST";
    
    public static final String ANY_FIELD = "_ANYFIELD_";
    
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
    
    // content functions
    public static final String TERM_OFFSET_MAP_JEXL_VARIABLE_NAME = "termOffsetMap";
    public static final String CONTENT_FUNCTION_NAMESPACE = "content";
    public static final String CONTENT_WITHIN_FUNCTION_NAME = "within";
    public static final String CONTENT_ADJACENT_FUNCTION_NAME = "adjacent";
    public static final String CONTENT_PHRASE_FUNCTION_NAME = "phrase";
    
    public static final String CONTENT_TERM_TIMESTAMP_KEY = "TERM_TIMESTAMP";
    public static final String CONTENT_TERM_POSITION_KEY = CONTENT_TERM_TIMESTAMP_KEY + ":POSITION";
    
    // RangeStream, temporary move
    public static final String SHARD_DAY_HINT = "SHARDS_AND_DAYS";
    
    // From evaluating iterator
    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";
    
    public static final String COLUMN_VISIBILITY = "columnVisibility";
}
