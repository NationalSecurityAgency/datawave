package datawave.ingest.metadata.id;

import java.util.regex.Matcher;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;

public class NamedMetadataIdParser extends MetadataIdParser {

    private String name = null;
    private String value = null;

    public NamedMetadataIdParser(String pattern, String name) {
        super(pattern);
        this.name = name;
    }

    public NamedMetadataIdParser(String pattern, String name, String value) {
        this(pattern, name);
        this.value = value;
    }

    @Override
    public void addMetadata(RawRecordContainer event, Multimap<String,String> metadata, String key) {
        Matcher matcher = getMatcher(key);
        if (matcher.matches()) {
            String fieldValue = (value == null ? matcher.group(1) : value);
            if (metadata != null) {
                metadata.put(name, fieldValue);
            }
        }
    }

}
