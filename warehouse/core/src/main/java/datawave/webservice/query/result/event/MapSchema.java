package datawave.webservice.query.result.event;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

public class MapSchema implements Schema<Map<String,String>> {
    public static MapSchema SCHEMA = new MapSchema();
    
    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return "entry";
            default:
                return null;
        }
    }
    
    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case "entry":
                return 1;
            default:
                return 0;
        }
    }
    
    @Override
    public boolean isInitialized(Map<String,String> message) {
        return true;
    }
    
    @Override
    public Map<String,String> newMessage() {
        return new HashMap<>();
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
    public Class<? super Map<String,String>> typeClass() {
        return Map.class;
    }
    
    @Override
    public void mergeFrom(Input input, Map<String,String> message) throws IOException {
        MapEntrySchema.MutableMapEntry entry = new MapEntrySchema.MutableMapEntry();
        int number;
        while ((number = input.readFieldNumber(this)) != 0) {
            switch (number) {
                case 1:
                    input.mergeObject(entry, MapEntrySchema.SCHEMA);
                    message.put(entry.getKey(), entry.getValue());
                    break;
                default:
                    input.handleUnknownField(number, this);
                    break;
            }
        }
    }
    
    @Override
    public void writeTo(Output output, Map<String,String> message) throws IOException {
        MapEntrySchema.DelegateMapEntry delegateMapEntry = new MapEntrySchema.DelegateMapEntry();
        for (Entry<String,String> entry : message.entrySet()) {
            delegateMapEntry.setDelegate(entry);
            output.writeObject(1, delegateMapEntry, MapEntrySchema.SCHEMA, true);
        }
    }
}
