package datawave.util.keyword;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates results from a keyword extraction algorithm for a single document and provides serialization and deserialization mechanism
 */
public class KeywordResults extends TagCloudInput {
    /** the name of the view from which the keywords were extracted */
    String view;

    /** the language of the source document used for keyword extraction */
    String language;

    public KeywordResults() {
        this("", "", "", "", new LinkedHashMap<>());
    }

    public KeywordResults(String source, String view, String language, String visibility, Map<String,Double> results) {
        super(source, visibility, results);
        this.view = view;
        this.language = language;
    }

    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
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

        KeywordResults otherKeywordResults = (KeywordResults) other;
        return Objects.equals(view, otherKeywordResults.view) && Objects.equals(language, otherKeywordResults.language);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), view, language);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String clazz = dataInput.readUTF();
        if (!clazz.equals(KeywordResults.class.getCanonicalName())) {
            throw new IllegalArgumentException("Incompatible DataInput");
        }

        super.readFields(dataInput);
        this.view = dataInput.readUTF();
        this.language = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // write the class first
        dataOutput.writeUTF(KeywordResults.class.getCanonicalName());
        super.write(dataOutput);
        dataOutput.writeUTF(view == null ? "" : view);
        dataOutput.writeUTF(language == null ? "" : language);
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
