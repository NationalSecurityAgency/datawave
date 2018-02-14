package datawave.ingest.mapreduce.handler.shard;

import com.google.common.collect.Multimap;
import datawave.util.MultimapSchema;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;

public class FieldIndexFilterData implements Message<FieldIndexFilterData> {
    
    /**
     * A mapping of fields to non-normalized values
     */
    private Multimap<String,String> fieldValueMapping;
    
    public FieldIndexFilterData() {
        this(null);
    }
    
    public FieldIndexFilterData(Multimap<String,String> fieldValueMapping) {
        this.fieldValueMapping = fieldValueMapping;
    }
    
    public Multimap<String,String> getFieldValueMapping() {
        return fieldValueMapping;
    }
    
    public void setFieldValueMapping(Multimap<String,String> fieldValueMapping) {
        this.fieldValueMapping = fieldValueMapping;
    }
    
    @Override
    public Schema<FieldIndexFilterData> cachedSchema() {
        return SCHEMA;
    }
    
    public static Schema<FieldIndexFilterData> SCHEMA = new Schema<FieldIndexFilterData>() {
        
        public static final String FIELD_VALUE_MAPPING = "fieldValueMapping";
        public Schema<Multimap<String,String>> stringMultimapSchema = new MultimapSchema();
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return FIELD_VALUE_MAPPING;
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            switch (name) {
                case FIELD_VALUE_MAPPING:
                    return 1;
                default:
                    return 0;
            }
        }
        
        @Override
        public boolean isInitialized(FieldIndexFilterData fieldIndexFilterData) {
            return true;
        }
        
        @Override
        public FieldIndexFilterData newMessage() {
            return new FieldIndexFilterData();
        }
        
        @Override
        public String messageName() {
            return FieldIndexFilterData.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return FieldIndexFilterData.class.getName();
        }
        
        @Override
        public Class<? super FieldIndexFilterData> typeClass() {
            return FieldIndexFilterData.class;
        }
        
        @Override
        public void mergeFrom(Input input, FieldIndexFilterData fieldIndexFilterData) throws IOException {
            for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        fieldIndexFilterData.setFieldValueMapping(input.mergeObject(null, stringMultimapSchema));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }
        
        @Override
        public void writeTo(Output output, FieldIndexFilterData fieldIndexFilterData) throws IOException {
            if (fieldIndexFilterData.getFieldValueMapping() != null)
                output.writeObject(1, fieldIndexFilterData.getFieldValueMapping(), stringMultimapSchema, false);
        }
    };
}
