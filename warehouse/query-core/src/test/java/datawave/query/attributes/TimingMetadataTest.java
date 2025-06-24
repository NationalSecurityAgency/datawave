package datawave.query.attributes;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TimingMetadataTest {

    private static final Logger log = LoggerFactory.getLogger(TimingMetadataTest.class);

    private static final int MAX_ITERATIONS = 1_000;

    private final Key docKey = new Key("row", "dt\0uid");

    private final Random random = new Random();
    private final int max = 12_000;

    @Test
    public void testSingle() {
        TimingMetadata metadata = createTimingMetadata();
        long totalRead = readKryo(metadata);
        long totalWrite = writeKryo(metadata);
        log.info("single standard read: {} write: {}", totalRead, totalWrite);
    }

    @Test
    public void testRandom() {
        TimingMetadata metadata = createRandomTimingMetadata();
        long totalRead = readKryo(metadata);
        long totalWrite = writeKryo(metadata);
        log.info("single random read: {} write: {}", totalRead, totalWrite);
    }

    @Test
    public void testBulkSingle() {
        long totalRead = 0;
        long totalWrite = 0;
        TimingMetadata metadata = createTimingMetadata();
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            totalRead += readKryo(metadata);
            totalWrite += writeKryo(metadata);
        }
        log.info("bulk standard read: {} write: {}", totalRead, totalWrite);
    }

    @Test
    public void testBulkRandom() {
        long totalRead = 0;
        long totalWrite = 0;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            TimingMetadata metadata = createRandomTimingMetadata();
            totalRead += readKryo(metadata);
            totalWrite += writeKryo(metadata);
        }
        log.info("bulk random read: {} write: {}", totalRead, totalWrite);
    }

    private TimingMetadata createTimingMetadata() {
        TimingMetadata metadata = new TimingMetadata();
        metadata.setMetadata(docKey);
        metadata.setToKeep(true);
        metadata.setNextCount(1234);
        metadata.setSourceCount(23);
        metadata.setSeekCount(34);
        metadata.setYieldCount(0);
        metadata.setHost("localhost");
        return metadata;
    }

    private TimingMetadata createRandomTimingMetadata() {
        TimingMetadata metadata = new TimingMetadata();
        metadata.setMetadata(docKey);
        metadata.setToKeep(true);
        metadata.setNextCount(random.nextInt(max));
        metadata.setSourceCount(random.nextInt(max));
        metadata.setSeekCount(random.nextInt(max));
        metadata.setYieldCount(random.nextInt(max));
        metadata.setHost("localhost");
        return metadata;
    }

    /**
     * A method that serializes the provided {@link TimingMetadata} using {@link TimingMetadata#write(Kryo, Output)} a number of times equal ot
     * {@link #MAX_ITERATIONS}.
     * <p>
     * The elapsed time is logged using the provided logger.
     *
     * @param metadata
     *            the TimingMetadata
     */
    protected long writeKryo(TimingMetadata metadata) {
        Kryo kryo = new Kryo();
        Output output = new Output(1024);

        long elapsed = 0;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            output.clear();
            long start = System.nanoTime();
            metadata.write(kryo, output);
            elapsed += System.nanoTime() - start;
        }
        return TimeUnit.NANOSECONDS.toMillis(elapsed);
    }

    /**
     * A method that deserializes the provided {@link TimingMetadata} using {@link TimingMetadata#read(Kryo, Input)} a number of times equal to
     * {@link #MAX_ITERATIONS}.
     * <p>
     * The elapsed time is logged using the provided logger.
     *
     * @param metadata
     *            the TimingMetadata
     */
    protected long readKryo(TimingMetadata metadata) {

        // first serialize
        Kryo kryo = new Kryo();
        Output output = new Output(1024);
        metadata.write(kryo, output);
        output.flush();
        byte[] data = output.getBuffer();

        long elapsed = 0;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try (Input input = new Input(data)) {
                TimingMetadata deserialized = new TimingMetadata();
                long start = System.nanoTime();
                deserialized.read(kryo, input);
                elapsed += System.nanoTime() - start;
            }
        }
        return TimeUnit.NANOSECONDS.toMillis(elapsed);
    }
}
