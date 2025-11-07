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

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

/**
 * Encapsulates results from a keyword extraction algorithm for a single document and provides serialization and deserialization mechanism
 */
public class KeywordResults implements Writable {

    private static final Gson gson = new Gson();

    /** the identifier for the source document */
    String source;

    /** the name of the view from which the keywords were extracted */
    String view;

    /** the language of the source document used for keyword extraction */
    String language;

    /** a visibility expression for these keyword results */
    String visibility;

    /** the keywords and scores produced by the extraction algorithm */
    final Map<String,Double> keywords;

    public KeywordResults() {
        this("", "", "", "", new LinkedHashMap<>());
    }

    public KeywordResults(String source, String view, String language, String visibility, Map<String,Double> results) {
        this.source = source;
        this.view = view;
        this.language = language;
        this.visibility = visibility;
        this.keywords = results;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
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

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public int getKeywordCount() {
        return keywords.size();
    }

    public Map<String,Double> getKeywords() {
        return keywords;
    }

    public String toJson() {
        return gson.toJson(this);
    }

    public static KeywordResults fromJson(String json) {
        return gson.fromJson(json, KeywordResults.class);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String clazz = dataInput.readUTF();
        if (!clazz.equals(KeywordResults.class.getCanonicalName())) {
            throw new IllegalArgumentException("Incompatible dataInput");
        }
        int sz = dataInput.readInt();
        this.source = dataInput.readUTF();
        this.view = dataInput.readUTF();
        this.language = dataInput.readUTF();
        this.visibility = dataInput.readUTF();
        for (int i = 0; i < sz; i++) {
            keywords.put(dataInput.readUTF(), dataInput.readDouble());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // write the class name first so reading the first field can easily test for compatibility
        dataOutput.writeUTF(KeywordResults.class.getCanonicalName());
        dataOutput.writeInt(keywords.size());
        dataOutput.writeUTF(source == null ? "" : source);
        dataOutput.writeUTF(view == null ? "" : view);
        dataOutput.writeUTF(language == null ? "" : language);
        dataOutput.writeUTF(visibility == null ? "" : visibility);
        for (Map.Entry<String,Double> e : keywords.entrySet()) {
            dataOutput.writeUTF(e.getKey());
            dataOutput.writeDouble(e.getValue());
        }
    }

    /**
     * Read the first string off the byte stream, it should be the class name if compatible
     *
     * @param bytes
     * @return true if this byte stream can be deserialized, false otherwise
     * @throws IOException
     */
    public static boolean canDeserialize(byte[] bytes) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes); DataInputStream dataInput = new DataInputStream(in)) {
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
