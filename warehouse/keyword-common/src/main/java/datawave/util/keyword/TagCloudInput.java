package datawave.util.keyword;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

/**
 * This is the base class used to generating TagClouds
 */
public class TagCloudInput implements Writable {
    private static final Gson gson = new Gson();

    public static String toJson(TagCloudInput input) {
        return gson.toJson(input);
    }

    public static TagCloudInput fromJson(String json) {
        return gson.fromJson(json, TagCloudInput.class);
    }

    public static TagCloudInput fromJson(String json, Class<? extends TagCloudInput> target) {
        return gson.fromJson(json, target);
    }

    private String source;
    private String visibility;
    private Map<String,Double> entities;
    private Map<String,String> metadata;

    private TagCloudInput() {
        // only to support deserialize()
    }

    public TagCloudInput(String source, String visibility, Map<String,Double> entities, Map<String,String> metadata) {
        this.source = source;
        this.visibility = visibility;
        this.entities = entities;
        this.metadata = metadata;
    }

    public Map<String,Double> getEntities() {
        return entities;
    }

    public String getVisibility() {
        return visibility;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Map<String,String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String,String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TagCloudInput)) {
            return false;
        }
        TagCloudInput otherTci = (TagCloudInput) other;
        return Objects.equals(source, otherTci.source) && Objects.equals(visibility, otherTci.visibility) && Objects.equals(entities, otherTci.entities)
                        && Objects.equals(metadata, otherTci.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, visibility, entities, metadata);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(source == null ? "" : source);
        dataOutput.writeUTF(visibility == null ? "" : visibility);
        dataOutput.writeInt(entities.size());
        for (Map.Entry<String,Double> e : entities.entrySet()) {
            dataOutput.writeUTF(e.getKey());
            dataOutput.writeDouble(e.getValue());
        }
        dataOutput.writeInt(metadata.size());
        for (Map.Entry<String,String> e : metadata.entrySet()) {
            dataOutput.writeUTF(e.getKey());
            dataOutput.writeUTF(e.getValue());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        source = dataInput.readUTF();
        visibility = dataInput.readUTF();
        int entityCount = dataInput.readInt();
        entities = new HashMap<>();
        for (int i = 0; i < entityCount; i++) {
            String label = dataInput.readUTF();
            double score = dataInput.readDouble();
            entities.put(label, score);
        }
        int metadataCount = dataInput.readInt();
        this.metadata = new HashMap<>();
        for (int i = 0; i < metadataCount; i++) {
            String key = dataInput.readUTF();
            String value = dataInput.readUTF();
            this.metadata.put(key, value);
        }
    }

    public static byte[] serialize(Writable results) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(out)) {
            results.write(dataOutput);
            out.flush();
            return out.toByteArray();
        }
    }

    public static TagCloudInput deserialize(byte[] input) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(input); DataInputStream dataInput = new DataInputStream(in)) {
            TagCloudInput results = new TagCloudInput();
            results.readFields(dataInput);
            return results;
        }
    }

    public static TagCloudInput deserialize(DataInput dataInput) throws IOException {
        TagCloudInput input = new TagCloudInput();
        input.readFields(dataInput);

        return input;
    }
}
