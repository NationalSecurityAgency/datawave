package datawave.query.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.type.ListType;
import datawave.data.type.Type;

/**
 * A base test class that provides support for integration testing of performance of TypeAttribute serialization and deserialization using either Kryo or Data
 * streams
 */
public abstract class TypeAttributeIT {

    private static final int MAX_ITERATIONS = 100_000;

    // normalization context
    protected static String NORMALIZED = "normalized";
    protected static String NON_NORMALIZED = "non-normalized";

    protected static final Key docKey = new Key("row", "dt\0uid");

    /**
     * Get the {@link Type} to use for each implementation of the test.
     *
     * @return the Type
     */
    protected abstract Type<?> getType();

    /**
     * Each extending class must provide its own implementation of a normalized attribute
     *
     * @return an Attribute built from normalized data
     */
    protected abstract TypeAttribute<?> createNormalizedAttribute();

    /**
     * Each extending class must provide its own implementation of a non-normalized attribute
     *
     * @return an Attribute built from non-normalized data
     */
    protected abstract TypeAttribute<?> createNonNormalizedAttribute();

    /**
     * Creates an attribute using the specific {@link Type} for each extending class.
     * <p>
     * Uses the standard {@link Type#setDelegateFromString(String)} to populate the delegate and normalized value.
     *
     * @param data
     *            the data
     * @return a TypeAttribute with a delegate Type
     */
    protected TypeAttribute<?> createAttribute(String data) {
        Type<?> type = getType();
        type.setDelegateFromString(data);
        return new TypeAttribute<>(type, docKey, false);
    }

    /**
     * A method that serializes the provided {@link TypeAttribute} using {@link TypeAttribute#write(Kryo, Output)} a number of times equal ot
     * {@link #MAX_ITERATIONS}.
     * <p>
     * The elapsed time is logged using the provided logger.
     *
     * @param context
     *            data normalization context
     * @param attribute
     *            the TypeAttribute
     * @param log
     *            the logger from a subclass
     */
    protected void writeKryo(String context, TypeAttribute<?> attribute, Logger log) {
        Kryo kryo = new Kryo();
        Output output = new Output(1024);

        long elapsed = 0;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            output.clear();
            long start = System.nanoTime();
            attribute.write(kryo, output);
            elapsed += System.nanoTime() - start;
        }
        log.info("kryo write ({}): {} ms", context, TimeUnit.NANOSECONDS.toMillis(elapsed));
    }

    /**
     * A method that deserializes the provided {@link TypeAttribute} using {@link TypeAttribute#read(Kryo, Input)} a number of times equal to
     * {@link #MAX_ITERATIONS}.
     * <p>
     * The elapsed time is logged using the provided logger.
     *
     * @param context
     *            data normalization context
     * @param attribute
     *            the TypeAttribute
     * @param log
     *            the logger from a subclass
     */
    protected void readKryo(String context, TypeAttribute<?> attribute, Logger log) {
        // first serialize
        Kryo kryo = new Kryo();
        Output output = new Output(1024);
        attribute.write(kryo, output);
        output.flush();
        byte[] data = output.getBuffer();

        long elapsed = 0;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try (Input input = new Input(data)) {
                TypeAttribute<?> deserialized = new TypeAttribute<>();
                long start = System.nanoTime();
                deserialized.read(kryo, input);
                elapsed += System.nanoTime() - start;
            }
        }
        log.info("kryo read ({}): {} ms", context, TimeUnit.NANOSECONDS.toMillis(elapsed));
    }

    /**
     * Read and write the provided {@link TypeAttribute} using Kryo, verifying the delegate and normalized value
     *
     * @param attribute
     *            the TypeAttribute
     */
    protected void readWriteKryo(TypeAttribute<?> attribute) {
        // The non-normalized value. Sometimes the input value is normalized but not every normalizer can denormalize.
        String delegateValue = attribute.getType().getDelegateAsString();
        // The normalized value. OneToManyNormalizers will return the original delegate value because of some weirdness.
        String normalizedValue = attribute.getType().getNormalizedValue();
        // The list of normalized values, only applicable when the Type is a OneToManyNormalizerType
        List<String> normalizedValues = null;
        if (attribute.getType() instanceof ListType) {
            normalizedValues = ((ListType) attribute.getType()).getNormalizedValues();
        }

        // first serialize
        Kryo kryo = new Kryo();
        Output output = new Output(1024);
        attribute.write(kryo, output);
        output.flush();
        byte[] data = output.getBuffer();

        try (Input input = new Input(data)) {
            TypeAttribute<?> deserialized = new TypeAttribute<>();
            deserialized.read(kryo, input);

            // validate delegate and normalized
            String deserializedDelegate = deserialized.getType().getDelegateAsString();
            String deserializedNormalized = deserialized.getType().getNormalizedValue();
            assertEquals(delegateValue, deserializedDelegate);
            assertEquals(normalizedValue, deserializedNormalized);

            if (deserialized.getType() instanceof ListType) {
                assertNotNull(normalizedValues, "delegate Type produced normalized values, but none were expected");
                assertEquals(normalizedValues, ((ListType) deserialized.getType()).getNormalizedValues());
            }
        }
    }

    /**
     * A method that serializes the provided {@link TypeAttribute} using {@link TypeAttribute#write(DataOutput)} a number of times equal to
     * {@link #MAX_ITERATIONS}
     *
     * @param context
     *            data normalization context
     * @param attribute
     *            the TypeAttribute
     * @param log
     *            the logger from a subclass
     */
    protected void writeDataOutput(String context, TypeAttribute<?> attribute, Logger log) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);

        long elapsed = 0;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try {
                long start = System.nanoTime();
                attribute.write(out);
                elapsed += System.nanoTime() - start;
            } catch (IOException e) {
                fail("Failed to write attribute: " + attribute, e);
                throw new RuntimeException(e);
            }
        }
        log.info("data write ({}): {}", context, TimeUnit.NANOSECONDS.toMillis(elapsed));
    }

    /**
     * A method that deserializes the provided {@link TypeAttribute} using {@link TypeAttribute#readFields(DataInput)} a number of times equal to
     * {@link #MAX_ITERATIONS}
     *
     * @param context
     *            data normalization context
     * @param attribute
     *            the TypeAttribute
     * @param log
     *            the logger from a subclass
     */
    protected void readDataInput(String context, TypeAttribute<?> attribute, Logger log) {
        byte[] data;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutput out = new DataOutputStream(baos);
            attribute.write(out);

            baos.flush();
            data = baos.toByteArray();
        } catch (IOException e) {
            fail("Failed to write attribute: " + attribute, e);
            throw new RuntimeException(e);
        }

        try {
            long elapsed = 0;
            for (int i = 0; i < MAX_ITERATIONS; i++) {
                DataInput in = new DataInputStream(new ByteArrayInputStream(data));
                TypeAttribute<?> deserialized = new TypeAttribute<>();
                long start = System.nanoTime();
                deserialized.readFields(in);
                elapsed += System.nanoTime() - start;
            }
            log.info("data read ({}): {}", context, TimeUnit.NANOSECONDS.toMillis(elapsed));
        } catch (IOException e) {
            fail("Failed to read attribute: " + attribute, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Read and write the provided {@link TypeAttribute} using DataInput/DataOutput, verifying the delegate and normalized value
     *
     * @param attribute
     *            the TypeAttribute
     */
    protected void readWriteData(TypeAttribute<?> attribute) {

        // The non-normalized value. Sometimes the input value is normalized but not every normalizer can denormalize.
        String delegateValue = attribute.getType().getDelegateAsString();
        // The normalized value. OneToManyNormalizers will return the original delegate value because of some weirdness.
        String normalizedValue = attribute.getType().getNormalizedValue();
        // The list of normalized values, only applicable when the Type is a OneToManyNormalizerType
        List<String> normalizedValues = null;
        if (attribute.getType() instanceof ListType) {
            normalizedValues = ((ListType) attribute.getType()).getNormalizedValues();
        }

        byte[] data;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutput out = new DataOutputStream(baos);
            attribute.write(out);
            baos.flush();
            data = baos.toByteArray();
        } catch (IOException e) {
            fail("Failed to write attribute: " + attribute, e);
            throw new RuntimeException(e);
        }

        try {
            DataInput in = new DataInputStream(new ByteArrayInputStream(data));
            TypeAttribute<?> deserialized = new TypeAttribute<>();
            deserialized.readFields(in);

            String deserializedDelegate = deserialized.getType().getDelegateAsString();
            String deserializedNormalized = deserialized.getType().getNormalizedValue();

            assertEquals(delegateValue, deserializedDelegate);
            assertEquals(normalizedValue, deserializedNormalized);
            if (deserialized.getType() instanceof ListType) {
                assertNotNull(normalizedValues, "delegate Type produced normalized values, but none were expected");
                assertEquals(normalizedValues, ((ListType) deserialized.getType()).getNormalizedValues());
            }
        } catch (IOException e) {
            fail("Failed to read attribute: " + attribute, e);
            throw new RuntimeException(e);
        }
    }

}
