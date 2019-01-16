package datawave.webservice.query.result.event;

import java.io.IOException;
import java.util.Map;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

public class MapEntrySchema implements Schema<MapEntrySchema.MutableMapEntry> {
    public static final MapEntrySchema SCHEMA = new MapEntrySchema();
    
    public static class MutableMapEntry implements Map.Entry<String,String> {
        private String key;
        private String value;
        
        @Override
        public String getKey() {
            return key;
        }
        
        public void setKey(String key) {
            this.key = key;
        }
        
        @Override
        public String getValue() {
            return value;
        }
        
        @Override
        public String setValue(String value) {
            String oldValue = this.value;
            this.value = value;
            return oldValue;
        }
    }
    
    public static class DelegateMapEntry extends MutableMapEntry {
        private Map.Entry<String,String> delegate;
        
        @Override
        public String getKey() {
            return delegate.getKey();
        }
        
        @Override
        public String getValue() {
            return delegate.getValue();
        }
        
        public void setDelegate(Map.Entry<String,String> delegate) {
            this.delegate = delegate;
        }
    }
    
    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return "key";
            case 2:
                return "value";
            default:
                return null;
        }
    }
    
    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case "key":
                return 1;
            case "value":
                return 2;
            default:
                return 0;
        }
    }
    
    @Override
    public boolean isInitialized(MutableMapEntry message) {
        return true;
    }
    
    @Override
    public MutableMapEntry newMessage() {
        return new MutableMapEntry();
    }
    
    @Override
    public String messageName() {
        return Map.class.getSimpleName();
    }
    
    @Override
    public String messageFullName() {
        return Map.class.getName();
    }
    
    @Override
    public Class<? super MutableMapEntry> typeClass() {
        return Map.Entry.class;
    }
    
    @Override
    public void mergeFrom(Input input, MutableMapEntry message) throws IOException {
        int number;
        while ((number = input.readFieldNumber(this)) != 0) {
            switch (number) {
                case 1:
                    message.setKey(input.readString());
                    break;
                case 2:
                    message.setValue(input.readString());
                    break;
                default:
                    input.handleUnknownField(number, this);
                    break;
            }
        }
    }
    
    @Override
    public void writeTo(Output output, MutableMapEntry message) throws IOException {
        output.writeString(1, message.getKey(), false);
        output.writeString(2, message.getValue(), false);
    }
}
