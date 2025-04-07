package datawave.query.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

// @formatter:off
/**
 * This class can be used to parse keys belonging to a model.
 *
 * Definitions:
 *   MODEL_NAME: the name of the model (e.g. "DATAWAVE")
 *   MODEL_FIELD: the model field name
 *   DB_FIELD: the field name as stored in the database
 *   ATTR_NAME: an attribute
 *   ATTR_VALUE: an attribute value
 *   ATTR_VALUES: a comma delimited list of 0 or more
 *   ATTR_VALUE ATTRIBUTE: either ATTR_NAME or ATTR_NAME=ATTR_VALUE
 *   ATTRIBUTES: a comma delimited list of 0 or more ATTRIBUTE
 *
 * Keys can have the following forms (row cf:cq value):
 *   MODEL_FIELD MODEL_NAME:DB_FIELD\x00"forward" ATTRIBUTES
 *   DB_FIELD MODEL_NAME:MODEL_FIELD\x00"reverse" ATTRIBUTES
 *   "model" MODEL_NAME:ATTRIBUTE
 *   "model" MODEL_NAME:ATTR_NAME ATTR_VALUES
 *   "model" MODEL_NAME:"attrs" ATTRIBUTES
 *   MODEL_FIELD MODEL_NAME:ATTRIBUTE
 *   MODEL_FIELD MODEL_NAME:ATTR_NAME ATTR_VALUES
 *   MODEL_FIELD MODEL_NAME:"attrs" ATTRIBUTES
 *
 * deprecated but parsable format:
 *   MODEL_FIELD MODEL_NAME:DB_FIELD\x00"index_only\x00"forward" ATTRIBUTES
 */
// @formatter:on
public class ModelKeyParser {
    
    public static final String NULL_BYTE = "\0";
    public static final Value NULL_VALUE = new Value(new byte[0]);
    
    @Deprecated
    public static final String INDEX_ONLY = "index_only";
    
    public static final String ATTRIBUTES = "attrs";
    
    public static final String MODEL = "model";
    
    private static Logger log = Logger.getLogger(ModelKeyParser.class);
    
    public static FieldMapping parseKey(Key key) {
        return parseKey(key, NULL_VALUE);
    }
    
    public static FieldMapping parseKey(Key key, Value value) {
        String row = key.getRow().toString();
        String[] colf = key.getColumnFamily().toString().split(NULL_BYTE);
        String[] colq = key.getColumnQualifier().toString().split(NULL_BYTE);
        String cv = key.getColumnVisibility().toString();
        
        String datatype = null;
        Direction direction;
        String dataField;
        String modelField;
        List<String> attributes = new ArrayList<>();
        
        if (colf.length == 1) {
            // no datatype, this is only the model name
        } else if (colf.length == 2) {
            datatype = colf[1];
        } else {
            throw new IllegalArgumentException("Key in unknown format, colf parts: " + colf.length);
        }
        
        // we can have attributes no matter the mapping
        attributes.addAll(Arrays.asList(StringUtils.split(new String(value.get(), StandardCharsets.UTF_8), ',')));
        
        if (1 == colq.length) {
            if (colq[0].isEmpty()) {
                throw new IllegalArgumentException("Expected a column qualifier for a model key: " + key);
            }
            // in this case we expect model or model field attributes
            if (!colq[0].equals(ATTRIBUTES)) {
                // in this case the colq is an attribute, or all of the attributes are related to the name in the colq
                if (attributes.isEmpty()) {
                    attributes.add(colq[0]);
                } else {
                    attributes.replaceAll(v -> colq[0] + '=' + v);
                }
            }
            modelField = (row.equals(MODEL) ? null : row);
            dataField = null;
            direction = null;
        } else if (2 == colq.length) {
            direction = Direction.getDirection(colq[1]);
            if (Direction.REVERSE.equals(direction)) {
                dataField = row;
                modelField = colq[0];
            } else {
                dataField = colq[0];
                modelField = row;
            }
        } else if (3 == colq.length && Direction.FORWARD == Direction.getDirection(colq[2])) {
            dataField = colq[0];
            modelField = row;
            direction = Direction.FORWARD; // we already checked it in the if condition
        } else {
            log.error("Error parsing key: " + key);
            throw new IllegalArgumentException("Key in unknown format, colq parts: " + colq.length);
        }
        
        return new FieldMapping(datatype, dataField, modelField, direction, cv, attributes);
    }
    
    public static Key createKey(FieldMapping mapping, String modelName) {
        ColumnVisibility cv = new ColumnVisibility(mapping.getColumnVisibility());
        
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        if (mapping.isFieldMapping()) {
            String inName = Direction.REVERSE.equals(mapping.getDirection()) ? mapping.getFieldName() : mapping.getModelFieldName();
            String outName = Direction.REVERSE.equals(mapping.getDirection()) ? mapping.getModelFieldName() : mapping.getFieldName();
            return new Key(inName, // Row
                            modelName + dataType, // ColFam
                            outName + NULL_BYTE + mapping.getDirection().getValue(), // ColQual
                            cv, // Visibility
                            System.currentTimeMillis() // Timestamp
            );
        } else {
            String fieldName = mapping.getModelFieldName() == null ? ModelKeyParser.MODEL : mapping.getModelFieldName();
            String attr = getAttrCqValue(mapping.getAttributes())[0];
            return new Key(fieldName, // Row
                            modelName + dataType, // ColFam
                            attr, // ColQ
                            cv, // Visibility
                            System.currentTimeMillis() // Timestamp
            );
        }
    }
    
    /**
     * Get the colq and the value for an attributes entry
     * 
     * @param attributes
     * @return A string array of 2: {colq, value}
     */
    private static String[] getAttrCqValue(Set<String> attributes) {
        Set<String> names = getAttrNames(attributes);
        if (names.size() == 0) {
            return new String[] {"", ""};
        } else if (names.size() == 1) {
            if (names.iterator().next() == null) {
                if (attributes.size() == 1) {
                    return new String[] {attributes.iterator().next(), ""};
                } else {
                    return new String[] {ModelKeyParser.ATTRIBUTES, Joiner.on(',').join(attributes)};
                }
            } else {
                return new String[] {names.iterator().next(), Joiner.on(',').join(getAttrValues(attributes))};
            }
        } else {
            return new String[] {ModelKeyParser.ATTRIBUTES, Joiner.on(',').join(attributes)};
        }
    }
    
    private static Set<String> getAttrNames(Set<String> attributes) {
        return attributes.stream().map(a -> (a.indexOf('=') >= 0 ? a.substring(0, a.indexOf('=')) : null)).collect(Collectors.toSet());
    }
    
    private static Set<String> getAttrValues(Set<String> attributes) {
        return attributes.stream().map(a -> (a.indexOf('=') >= 0 ? a.substring(a.indexOf('=') + 1) : a)).collect(Collectors.toSet());
    }
    
    public static Mutation createMutation(FieldMapping mapping, String modelName) {
        ColumnVisibility cv = new ColumnVisibility(mapping.getColumnVisibility());
        Mutation m;
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        
        if (mapping.isFieldMapping()) {
            if (Direction.REVERSE.equals(mapping.getDirection())) {
                // Reverse mappings should not have indexOnly designators. If they do, scrub it off.
                m = new Mutation(mapping.getFieldName());
                m.put(modelName + dataType, mapping.getModelFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis(),
                                NULL_VALUE);
            } else {
                m = new Mutation(mapping.getModelFieldName());
                m.put(modelName + dataType, mapping.getFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis(), NULL_VALUE);
            }
        } else {
            String fieldName = mapping.getModelFieldName() == null ? ModelKeyParser.MODEL : mapping.getModelFieldName();
            String[] attr = getAttrCqValue(mapping.getAttributes());
            m = new Mutation(fieldName);
            m.put(modelName + dataType, attr[0], cv, System.currentTimeMillis(), new Value(attr[1]));
        }
        return m;
    }
    
    public static Mutation createDeleteMutation(FieldMapping mapping, String modelName) {
        ColumnVisibility cv = new ColumnVisibility(mapping.getColumnVisibility());
        Mutation m;
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        
        if (mapping.isFieldMapping()) {
            if (Direction.REVERSE.equals(mapping.getDirection())) {
                m = new Mutation(mapping.getFieldName());
                m.putDelete(modelName + dataType, mapping.getModelFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis());
            } else {
                m = new Mutation(mapping.getModelFieldName());
                m.putDelete(modelName + dataType, mapping.getFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis());
                m.putDelete(modelName + dataType, mapping.getFieldName() + NULL_BYTE + INDEX_ONLY + NULL_BYTE + mapping.getDirection().getValue(), cv,
                                System.currentTimeMillis());
            }
        } else {
            String fieldName = mapping.getModelFieldName() == null ? ModelKeyParser.MODEL : mapping.getModelFieldName();
            m = new Mutation(fieldName);
            m.putDelete(modelName + dataType, ATTRIBUTES, cv, System.currentTimeMillis());
            if (mapping.getAttributes().isEmpty()) {
                m.putDelete(modelName + dataType, "", cv, System.currentTimeMillis());
            } else {
                for (String attr : mapping.getAttributes()) {
                    m.putDelete(modelName + dataType, attr, cv, System.currentTimeMillis());
                    if (attr.indexOf('=') >= 0) {
                        m.putDelete(modelName + dataType, attr.substring(0, attr.indexOf('=')), cv, System.currentTimeMillis());
                    }
                }
            }
        }
        return m;
    }
}
