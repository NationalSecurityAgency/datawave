package datawave.util.keyword;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encapsulates results from a keyword extraction algorithm for a single document and provides serialization and deserialization mechanism
 */
public class KeywordResults extends TagCloudInput {
    // TODO-crwill9 this class should be decoupled from TagCloudInput
    public KeywordResults() {
        this("", "", new LinkedHashMap<>(), new HashMap<>());
    }

    public KeywordResults(String source, String visibility, Map<String,Double> results, Map<String,String> metadata) {
        super(source, visibility, results, metadata);
    }

    public Map<String,Double> getKeywords() {
        return getEntities();
    }

    public int getKeywordCount() {
        return getEntities().size();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof KeywordResults)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String clazz = dataInput.readUTF();
        if (!clazz.equals(KeywordResults.class.getCanonicalName())) {
            throw new IllegalArgumentException("Incompatible DataInput");
        }

        super.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // write the class first
        dataOutput.writeUTF(KeywordResults.class.getCanonicalName());
        super.write(dataOutput);
    }

    public static boolean canDeserialize(byte[] input) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(input); DataInputStream dataInput = new DataInputStream(in)) {
            return dataInput.readUTF().equals(KeywordResults.class.getCanonicalName());
        }
    }

    public static byte[] serialize(KeywordResults results) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(out)) {
            results.write(dataOutput);
            out.flush();
            return out.toByteArray();
        }
    }

    public static KeywordResults deserialize(byte[] input) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(input); DataInputStream dataInput = new DataInputStream(in)) {
            KeywordResults results = new KeywordResults();
            results.readFields(dataInput);
            return results;
        }
    }
}
