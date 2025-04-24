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

/**
 * Encapsulates results from a keyword extraction algorithm and provides serialization and deserialization mechanism
 */
public class KeywordResults implements Writable {

    String source;
    final LinkedHashMap<String,Double> results;

    public KeywordResults() {
        this("", new LinkedHashMap<>());
    }

    public KeywordResults(String source, LinkedHashMap<String,Double> results) {
        this.source = source;
        this.results = results;
    }

    public String getSource() {
        return source;
    }

    public int size() {
        return results.size();
    }

    public LinkedHashMap<String,Double> get() {
        return results;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int sz = dataInput.readInt();
        this.source = dataInput.readUTF();
        for (int i = 0; i < sz; i++) {
            results.put(dataInput.readUTF(), dataInput.readDouble());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(results.size());
        dataOutput.writeUTF(source == null ? "" : source);
        for (Map.Entry<String,Double> e : results.entrySet()) {
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
