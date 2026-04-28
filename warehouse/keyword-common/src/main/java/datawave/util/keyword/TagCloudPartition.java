package datawave.util.keyword;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

/**
 * Used to partition TagCloudInput
 */
public class TagCloudPartition implements Writable {
    public enum ScoreType {
        LOWER_IS_BETTER, HIGHER_IS_BETTER
    }

    private static final String EMPTY_PARTITION = "";

    private String partition;
    // TODO-crwill9 this may not be necessary
    private String label;
    private List<TagCloudInput> inputs;
    private ScoreType scoreType;

    public TagCloudPartition() {
        this(EMPTY_PARTITION);
    }

    public TagCloudPartition(String partition) {
        this(partition, partition);
    }

    public TagCloudPartition(String partition, String label) {
        this(partition, label, new ArrayList<>());
    }

    public TagCloudPartition(String partition, String label, List<TagCloudInput> inputs) {
        this(partition, label, ScoreType.LOWER_IS_BETTER, inputs);
    }

    public TagCloudPartition(String partition, String label, ScoreType scoreType, List<TagCloudInput> inputs) {
        this.partition = partition;
        this.label = label;
        this.scoreType = scoreType;
        this.inputs = inputs;
    }

    public void addInput(TagCloudInput input) {
        inputs.add(input);
    }

    public String getPartition() {
        return partition;
    }

    public String getLabel() {
        return label;
    }

    public List<TagCloudInput> getInputs() {
        return inputs;
    }

    public ScoreType getScoreType() {
        return scoreType;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TagCloudPartition)) {
            return false;
        }
        TagCloudPartition otherPartition = (TagCloudPartition) other;
        return Objects.equals(partition, otherPartition.partition) && Objects.equals(label, otherPartition.label)
                        && Objects.equals(scoreType, otherPartition.scoreType) && Objects.equals(inputs, otherPartition.inputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, label, scoreType, inputs);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(TagCloudPartition.class.getCanonicalName());
        dataOutput.writeUTF(this.partition);
        dataOutput.writeUTF(this.label);
        dataOutput.writeUTF(this.scoreType.name());
        dataOutput.writeInt(this.inputs.size());
        for (TagCloudInput input : this.inputs) {
            input.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String clazz = dataInput.readUTF();
        if (!clazz.equals(TagCloudPartition.class.getCanonicalName())) {
            throw new IllegalArgumentException("Incompatible DataInput");
        }
        this.partition = dataInput.readUTF();
        this.label = dataInput.readUTF();
        String scoreTypeName = dataInput.readUTF();
        this.scoreType = ScoreType.valueOf(scoreTypeName);
        int inputCount = dataInput.readInt();
        this.inputs = new ArrayList<>();
        for (int i = 0; i < inputCount; i++) {
            TagCloudInput input = TagCloudInput.deserialize(dataInput);
            this.inputs.add(input);
        }
    }

    public static boolean canDeserialize(byte[] input) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(input); DataInputStream dataInput = new DataInputStream(in)) {
            return dataInput.readUTF().equals(TagCloudPartition.class.getCanonicalName());
        }
    }

    public static TagCloudPartition deserialize(byte[] input) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(input); DataInputStream dataInput = new DataInputStream(in)) {
            TagCloudPartition partition = new TagCloudPartition();
            partition.readFields(dataInput);
            return partition;
        }
    }

    public static byte[] serialize(Writable writable) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(out)) {
            writable.write(dataOutput);
            out.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
