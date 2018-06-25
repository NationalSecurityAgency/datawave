package datawave.query.testframework;

import org.apache.log4j.Logger;
import org.junit.Assert;

import java.lang.reflect.Type;

/**
 * Parse a string into a set of tokens that represent a <code>KEY</code> <code>OP</code> <code>VALUE</code>. Valid operation values are specified by
 * {@link QueryOp}.
 * <p>
 * Examples
 * </p>
 * <ul>
 * <li>a == 'abc'</li>
 * <li>a =~ 'a.*'</li>
 * <li>n > 2</li>
 * </ul>
 * </ul>
 */
public class QueryAction {
    
    private static final Logger log = Logger.getLogger(QueryAction.class);
    
    /**
     * Defines the JEXL operator tokens that appear for a simple query string (e.g. a == b).
     */
    enum QueryOp {
        EQUAL("=="), GT(">"), GTE(">="), LT("<"), LTE("<="), NOT_EQUAL("!="), REGEX("=~"), NEG_REGEX("!~"), UNKNOWN("unknown");
        
        private final String token;
        
        QueryOp(String op) {
            this.token = op;
        }
        
        static QueryOp getOp(final String op, final boolean mustBeValid) {
            for (final QueryOp val : QueryOp.values()) {
                if (val.token.equalsIgnoreCase(op)) {
                    return val;
                }
            }
            
            if (mustBeValid) {
                throw new AssertionError("invalid operand(" + op + ")");
            }
            log.warn("operator (" + op + ") is not a known operand");
            return UNKNOWN;
        }
    }
    
    private String key;
    private String value;
    private QueryOp op;
    
    /**
     * Creates a simple query action object.
     * 
     * @param field
     *            field for query
     * @param op
     *            operation
     * @param valStr
     *            value for operation
     * @param dataType
     *            type of data
     * @param validOp
     *            when true an error wil result for invalid op
     */
    public QueryAction(final String field, final String op, final String valStr, final Type dataType, final boolean validOp) {
        this.key = field;
        this.op = QueryOp.getOp(op, validOp);
        if (dataType.getTypeName().equalsIgnoreCase("java.lang.String")) {
            if (valStr.startsWith("'") && valStr.endsWith("'")) {
                this.value = valStr.substring(1, valStr.length() - 1);
            } else {
                Assert.fail("string value must be enclosed in quotes(" + valStr + ")");
            }
        } else {
            this.value = valStr;
        }
    }
    
    public String getKey() {
        return key;
    }
    
    public String getValue() {
        return value;
    }
    
    public QueryOp getOp() {
        return op;
    }
    
    @Override
    public String toString() {
        return "QueryAction{" + "key='" + key + '\'' + ", value='" + value + '\'' + ", op=" + op + '}';
    }
}
