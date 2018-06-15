package datawave.query.testframework;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Base class for all POJO managers. A POJO Manager is responsible for managing the data for a datatype.
 */
public abstract class AbstractPOJOManager implements IRawDataManager {
    
    protected final String rawKeyField;
    /**
     * Contains all test entries that should exist in Accumulo.
     */
    protected final Set<IRawData> pojos;
    
    /**
     *
     * @param keyField
     *            key field returned for results validation
     */
    AbstractPOJOManager(final String keyField) {
        this.rawKeyField = keyField;
        this.pojos = new HashSet<>();
    }
    
    /**
     * Finds all matching entries in the raw dataset that match the specified query entry.
     *
     * @param fieldList
     *            list of fields for matching
     * @param fieldTypes
     *            mapping of field to type
     * @param action
     *            parsed query action
     * @return set of matching raw data entries
     */
    protected Set<IRawData> matchField(final Collection<String> fieldList, final Map<String,Type> fieldTypes, final QueryAction action,
                    final Collection<IRawData> data) {
        Set<IRawData> match = new HashSet<>();
        
        for (final String field : fieldList) {
            IQueryResolver resolver;
            Type type = fieldTypes.get(field);
            switch (type.getTypeName()) {
                case "java.lang.String":
                    resolver = new StringResolver();
                    break;
                case "java.lang.Integer":
                    resolver = new IntegerResolver();
                    break;
                default:
                    throw new AssertionError("unknown field type(" + type.getTypeName() + ")");
            }
            switch (action.getOp()) {
                case EQUAL:
                    match.addAll(resolver.isEqual(field, action.getValue(), data));
                    break;
                case NOT_EQUAL:
                    match.addAll(resolver.notEqual(field, action.getValue(), data));
                    break;
                case REGEX:
                    match.addAll(resolver.regex(field, action.getValue(), data));
                    break;
                case NEG_REGEX:
                    match.addAll(resolver.negRegex(field, action.getValue(), data));
                    break;
                case GT:
                    match.addAll(resolver.greater(field, action.getValue(), data));
                    break;
                case GTE:
                    match.addAll(resolver.greaterEqual(field, action.getValue(), data));
                    break;
                case LT:
                    match.addAll(resolver.less(field, action.getValue(), data));
                    break;
                case LTE:
                    match.addAll(resolver.lessEqual(field, action.getValue(), data));
                    break;
            }
        }
        
        return match;
    }
}
