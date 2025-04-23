package datawave.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.ProtostuffException;
import io.protostuff.Schema;

/**
 * This schema can be used by the protostuff api to serialize/deserialize a String Multimap
 */
public class StringMultimapSchema implements Schema<Multimap<String,String>> {
    
    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return "e";
            default:
                return null;
        }
    }
    
    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case "e":
                return 1;
            default:
                return 0;
        }
    }
    
    @Override
    public boolean isInitialized(Multimap<String,String> message) {
        return true;
    }
    
    @Override
    public Multimap<String,String> newMessage() {
        return ArrayListMultimap.create();
    }
    
    @Override
    public String messageName() {
        return Multimap.class.getSimpleName();
    }
    
    @Override
    public String messageFullName() {
        return Multimap.class.getName();
    }
    
    @Override
    public Class<? super Multimap<String,String>> typeClass() {
        return Multimap.class;
    }
    
    @Override
    public void mergeFrom(Input input, Multimap<String,String> multimap) throws IOException {
        for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
            switch (number) {
                case 0:
                    return;
                case 1:
                    Entry entry = input.mergeObject(null, entrySchema);
                    multimap.putAll(entry.getKey(), entry.getValues());
                    break;
                default:
                    throw new ProtostuffException("The map was incorrectly serialized.");
            }
        }
    }
    
    @Override
    public void writeTo(Output output, Multimap<String,String> multimap) throws IOException {
        if (multimap != null)
            for (String key : multimap.keySet())
                output.writeObject(1, new Entry(key, multimap.get(key)), entrySchema, true);
    }
    
    private static class Entry {
        private String key;
        private Collection<String> values;
        
        public Entry() {
            this.values = new ArrayList<>();
        }
        
        public Entry(String key, Collection<String> values) {
            this.key = key;
            this.values = values;
        }
        
        public String getKey() {
            return key;
        }
        
        public void setKey(String key) {
            this.key = key;
        }
        
        public Collection<String> getValues() {
            return values;
        }
        
        public void setValues(Collection<String> values) {
            this.values = values;
        }
        
        public void addValue(String value) {
            this.values.add(value);
        }
    }
    
    private static Schema<Entry> entrySchema = new Schema<Entry>() {
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "k";
                case 2:
                    return "v";
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            switch (name) {
                case "k":
                    return 1;
                case "v":
                    return 2;
                default:
                    return 0;
            }
        }
        
        @Override
        public boolean isInitialized(Entry entry) {
            return true;
        }
        
        @Override
        public Entry newMessage() {
            return new Entry();
        }
        
        @Override
        public String messageName() {
            return Entry.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return Entry.class.getName();
        }
        
        @Override
        public Class<? super Entry> typeClass() {
            return Entry.class;
        }
        
        @Override
        public void mergeFrom(Input input, Entry entry) throws IOException {
            for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        entry.setKey(input.readString());
                        break;
                    case 2:
                        entry.addValue(input.readString());
                        break;
                    default:
                        throw new ProtostuffException("The map was incorrectly serialized.");
                }
            }
        }
        
        @Override
        public void writeTo(Output output, Entry entry) throws IOException {
            if (entry.getKey() != null)
                output.writeString(1, entry.getKey(), false);
            if (entry.getValues() != null)
                for (String value : entry.getValues())
                    output.writeString(2, value, true);
        }
    };
}
