package datawave.test.framework.generators.field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.base.Preconditions;

/**
 * A field name generator that produces numeric field names such as <code>1_1, 2_1, 3_1</code>
 * <p>
 * A numeric field name requires the JEXL identifier prefix when written into a query, for example <code>$1_1 == 'value'</code>
 * <p>
 * The {@link Random} is supplied by the caller rather than created here so that an entire ingest can be reproduced from a single seed. See
 * {@code IngestMetadataBuilder.setSeed(long)}.
 */
public class NumericFieldNameGenerator implements FieldNameGenerator {

    private final List<String> fieldNames = new ArrayList<>();
    private final Random random;

    /**
     * Create a generator of numeric field names
     *
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static NumericFieldNameGenerator create(Random random) {
        return new NumericFieldNameGenerator(random);
    }

    private NumericFieldNameGenerator(Random random) {
        Preconditions.checkNotNull(random, "random cannot be null");
        this.random = random;
    }

    public void generate(int n) {
        int start = fieldNames.size() + 1;
        for (int i = start; i < start + n; i++) {
            fieldNames.add(i + "_1");
        }
    }

    public List<String> getFieldNames() {
        return Collections.unmodifiableList(fieldNames);
    }

    public List<String> getRandomizedFieldNames() {
        List<String> randomized = new ArrayList<>(fieldNames);
        Collections.shuffle(randomized, random);
        return randomized;
    }

    @Override
    public boolean isNumeric() {
        return true;
    }
}
