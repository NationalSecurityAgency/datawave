package datawave.ingest.mapreduce.handler.facet;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.accumulo.core.data.Value;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FacetValue extends Value {
    private HyperLogLogPlus cardinalityEstimate;
    private CountMinSketch frequencyEstimate;
    protected final AtomicBoolean written = new AtomicBoolean(false);

    public FacetValue(HyperLogLogPlus cardinalityEstimate, CountMinSketch frequencyEstimate) {
        this.cardinalityEstimate = cardinalityEstimate;
        this.frequencyEstimate = frequencyEstimate;
    }

    public FacetValue(HyperLogLogPlus cardinalityEstimate) {
        this(cardinalityEstimate, null);
    }

    protected FacetValue() {

    }

    public static FacetValue buildFrom(final DataInput in) throws IOException {
        FacetValue value = new FacetValue();
        value.readFields(in);
        return value;
    }

    public byte[] get() {
        try {
            ensureWritten();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return super.get();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int cardSize = in.readInt();
        byte[] cardinality = new byte[cardSize];
        in.readFully(cardinality);

        // TODO: frequencyEstimate
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ensureWritten();
        super.write(out);
    }

    private void ensureWritten() throws IOException {
        if (!written.get()) {
            synchronized (written) {
                if (!written.get()) {
                    ByteArrayOutputStream bout = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(bout);
                    byte[] cardinality = cardinalityEstimate.getBytes();

                    out.writeInt(cardinality.length);
                    out.write(cardinality, 0, cardinality.length);

                    if (null != frequencyEstimate) {
                        byte[] frequency = CountMinSketch.serialize(frequencyEstimate);
                        out.writeInt(frequency.length);
                        out.write(frequency, 0, frequency.length);
                    }

                    out.close();
                    value = bout.toByteArray();
                }
            }
        }
    }
}
