package datawave.query.util.keyword;

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

    /** the keywords and scores produced by the extraction algorithm */
    final LinkedHashMap<String,Double> keywords;

    public KeywordResults() {
        this("", "", "", new LinkedHashMap<>());
    }

    public KeywordResults(String source, String view, String language, LinkedHashMap<String,Double> results) {
        this.source = source;
        this.view = view;
        this.language = language;
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

    public int getKeywordCount() {
        return keywords.size();
    }

    public LinkedHashMap<String,Double> getKeywords() {
        return keywords;
    }

    public String getKeywordsAsJson() {
        return gson.toJson(keywords);
    }

    public String toJson() {
        return gson.toJson(this);
    }

    public static KeywordResults fromJson(String json) {
        return gson.fromJson(json, KeywordResults.class);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int sz = dataInput.readInt();
        this.source = dataInput.readUTF();
        this.view = dataInput.readUTF();
        this.language = dataInput.readUTF();
        for (int i = 0; i < sz; i++) {
            keywords.put(dataInput.readUTF(), dataInput.readDouble());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(keywords.size());
        dataOutput.writeUTF(source == null ? "" : source);
        dataOutput.writeUTF(view == null ? "" : view);
        dataOutput.writeUTF(language == null ? "" : language);
        for (Map.Entry<String,Double> e : keywords.entrySet()) {
            dataOutput.writeUTF(e.getKey());
            dataOutput.writeDouble(e.getValue());
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
