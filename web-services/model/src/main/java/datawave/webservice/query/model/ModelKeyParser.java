package datawave.webservice.query.model;

import datawave.webservice.model.Direction;
import datawave.webservice.model.FieldMapping;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ModelKeyParser {
    
    public static final String NULL_BYTE = "\0";
    public static final Value NULL_VALUE = new Value(new byte[0]);
    
    private static final Logger log = Logger.getLogger(ModelKeyParser.class);
    
    public static FieldMapping parseKey(Key key) {
        String row = key.getRow().toString();
        String[] colq = key.getColumnQualifier().toString().split(NULL_BYTE);
        String visibility = key.getColumnVisibility().toString();
        String[] colf = key.getColumnFamily().toString().split(NULL_BYTE);
        
        String datatype = null;
        Direction direction;
        String dataField;
        String modelField;
        
        // The column family length may be 1 (older style with no datatype) or 2 (contains a datatype).
        if (colf.length == 2) {
            datatype = colf[1];
        } else if (colf.length > 2) {
            throw new IllegalArgumentException("Key in unknown format, colf parts: " + colf.length);
        }
        
        if (2 == colq.length) {
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
        
        FieldMapping mapping = new FieldMapping();
        mapping.setColumnVisibility(visibility);
        mapping.setDatatype(datatype);
        mapping.setDirection(direction);
        mapping.setFieldName(dataField);
        mapping.setModelFieldName(modelField);
        return mapping;
    }
    
    public static Key createKey(FieldMapping mapping, String modelName) {
        ColumnVisibility visibility = new ColumnVisibility(mapping.getColumnVisibility());
        String inName = Direction.REVERSE.equals(mapping.getDirection()) ? mapping.getFieldName() : mapping.getModelFieldName();
        String outName = Direction.REVERSE.equals(mapping.getDirection()) ? mapping.getModelFieldName() : mapping.getFieldName();
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        return new Key(inName, modelName + dataType, outName + NULL_BYTE + mapping.getDirection().getValue(), visibility, System.currentTimeMillis());
    }
    
    public static Mutation createMutation(FieldMapping mapping, String modelName) {
        ColumnVisibility visibility = new ColumnVisibility(mapping.getColumnVisibility());
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        
        Mutation mutation;
        if (Direction.REVERSE.equals(mapping.getDirection())) {
            mutation = new Mutation(mapping.getFieldName());
            mutation.put(modelName + dataType, mapping.getModelFieldName() + NULL_BYTE + mapping.getDirection().getValue(), visibility,
                            System.currentTimeMillis(), NULL_VALUE);
        } else {
            mutation = new Mutation(mapping.getModelFieldName());
            mutation.put(modelName + dataType, mapping.getFieldName() + NULL_BYTE + mapping.getDirection().getValue(), visibility, System.currentTimeMillis(),
                            NULL_VALUE);
        }
        return mutation;
    }
    
    public static Mutation createDeleteMutation(FieldMapping mapping, String modelName) {
        ColumnVisibility visibility = new ColumnVisibility(mapping.getColumnVisibility());
        Mutation mutation;
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        
        if (Direction.REVERSE.equals(mapping.getDirection())) {
            mutation = new Mutation(mapping.getFieldName());
            mutation.putDelete(modelName + dataType, mapping.getModelFieldName() + NULL_BYTE + mapping.getDirection().getValue(), visibility,
                            System.currentTimeMillis());
        } else {
            mutation = new Mutation(mapping.getModelFieldName());
            mutation.putDelete(modelName + dataType, mapping.getFieldName() + NULL_BYTE + mapping.getDirection().getValue(), visibility,
                            System.currentTimeMillis());
            mutation.putDelete(modelName + dataType, mapping.getFieldName() + NULL_BYTE + "index_only" + NULL_BYTE + mapping.getDirection().getValue(),
                            visibility, System.currentTimeMillis());
        }
        return mutation;
    }
}
