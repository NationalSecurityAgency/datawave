package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.test.framework.generators.field.AlphabeticFieldNameGenerator;
import datawave.test.framework.generators.field.FieldNameGenerator;
import datawave.test.framework.generators.field.NumericFieldNameGenerator;
import datawave.test.framework.generators.id.ModuloEventIdGenerator;
import datawave.test.framework.generators.id.SequentialEventIdGenerator;
import datawave.test.framework.generators.value.LinearNumberGenerator;
import datawave.test.framework.generators.value.PhraseGenerator;
import datawave.test.framework.generators.value.RandomAlphabeticGenerator;
import datawave.test.framework.generators.value.RandomNumericGenerator;
import datawave.test.framework.generators.value.ValueGenerator;
import datawave.test.framework.util.Combination;
import datawave.test.framework.util.InfiniteIterator;
import datawave.test.framework.util.MetadataColumn;

/**
 * Deterministically generates a list of {@link FieldMetadata} used for test data and queries.
 */
public class IngestMetadata {

    private static final Logger log = LoggerFactory.getLogger(IngestMetadata.class);

    private static final int DEFAULT_EVENT_COUNT = 25;
    private static final int DEFAULT_VALUES_PER_FIELD = 2;
    public static final int DEFAULT_NUM_SHARDS = 10;

    /**
     * Each eligible (metadata column combination, normalizer) pairing produces this many fields - one per event id offset - so that two fields sharing a shape
     * can still land on overlapping-but-distinct event id sets.
     */
    private static final int FIELDS_PER_PAIRING = 2;

    /**
     * The field name of the synthetic, per-event unique identifier field. Callers that need to build queries against it should use
     * {@code QueryGenerator.singleTermId} rather than looking it up by this name directly.
     */
    public static final String ID_FIELD_NAME = "ID";

    private final boolean alphabeticFieldsEnabled;
    private final boolean numericFieldsEnabled;
    private final int numShards;
    private final List<MetadataColumn> baseMetadataColumns;
    private final List<String> baseDatatypes = List.of("datatype-a");
    private final List<Type<?>> baseNormalizers;

    private final long seed;
    private final Random random;

    private final List<FieldMetadata> fieldMetadata = new ArrayList<>();

    private int eventCount = DEFAULT_EVENT_COUNT;

    /**
     * Default constructor for IngestMetadata
     *
     * @param baseMetadataColumns
     *            the metadata columns used
     * @param baseNormalizers
     *            the normalizers used
     * @param alphabeticFieldsEnabled
     *            flag that enables alphabetic fields
     * @param numericFieldsEnabled
     *            flag that enables numeric fields
     * @param numShards
     *            the number of shards to distribute events across
     * @param seed
     *            the seed every generated value derives from, logged by {@link #createEvents(int, int)} so a failing run can be reproduced
     */
    public IngestMetadata(List<MetadataColumn> baseMetadataColumns, List<Type<?>> baseNormalizers, boolean alphabeticFieldsEnabled,
                    boolean numericFieldsEnabled, int numShards, long seed) {
        Preconditions.checkArgument(numShards > 0, "numShards must be greater than 0");
        Preconditions.checkArgument(alphabeticFieldsEnabled || numericFieldsEnabled, "alphabetic or numeric fields must be enabled");
        for (Type<?> normalizer : baseNormalizers) {
            Preconditions.checkArgument(isSupportedNormalizer(normalizer), "normalizer not supported: %s", normalizer.getClass().getName());
        }
        this.baseMetadataColumns = baseMetadataColumns;
        this.baseNormalizers = baseNormalizers;
        this.alphabeticFieldsEnabled = alphabeticFieldsEnabled;
        this.numericFieldsEnabled = numericFieldsEnabled;
        this.numShards = numShards;
        this.seed = seed;
        this.random = new Random(seed);
    }

    /**
     * The seed every generated value derives from. Passing it back through {@link IngestMetadataBuilder#setSeed(long)} reproduces this instance's data exactly.
     *
     * @return the seed
     */
    public long getSeed() {
        return seed;
    }

    /**
     * Plan the number of unique {@link FieldMetadata} entries given the input metadata columns, normalizers, and field types.
     * <p>
     * This is the exact size of {@link #getFieldMetadata()} after {@link #createEvents()}: every eligible (metadata column combination, normalizer) pairing
     * yields {@link #FIELDS_PER_PAIRING} fields per enabled field name type, and the synthetic {@link #ID_FIELD_NAME} field is always added on top.
     *
     * @return the number of unique {@link FieldMetadata} entries
     */
    public int plan() {
        // metadata * normalizers * field types, excluding TF combos paired with a non-text normalizer. Normalizers do not get combinatoric.
        int generatedFields = countEligiblePairings(Combination.getAllCombinations(baseMetadataColumns)) * FIELDS_PER_PAIRING * enabledFieldTypes();
        // the synthetic ID field is created unconditionally, outside the combinatoric field space
        return generatedFields + 1;
    }

    /**
     * The number of field name types in play, which multiplies the field space.
     *
     * @return 1 if only alphabetic or only numeric field names are enabled, 2 if both are
     */
    private int enabledFieldTypes() {
        int enabledFieldTypes = 0;
        if (numericFieldsEnabled) {
            enabledFieldTypes++;
        }
        if (alphabeticFieldsEnabled) {
            enabledFieldTypes++;
        }
        return enabledFieldTypes;
    }

    /**
     * Count the (metadataColumn combo, normalizer) pairings eligible for field generation.
     * <p>
     * A combo containing {@link MetadataColumn#TF} requires a phrase value, so it is only eligible when paired with {@link LcNoDiacriticsType}; other
     * normalizers are skipped for that combo since a phrase generator does not apply to them.
     *
     * @param metadataColumns
     *            all combinations of the base metadata columns
     * @return the number of eligible (combo, normalizer) pairings
     */
    private int countEligiblePairings(List<List<MetadataColumn>> metadataColumns) {
        int eligiblePairings = 0;
        for (List<MetadataColumn> combo : metadataColumns) {
            for (Type<?> normalizer : baseNormalizers) {
                if (combo.contains(TF) && !(normalizer instanceof LcNoDiacriticsType)) {
                    continue;
                }
                eligiblePairings++;
            }
        }
        return eligiblePairings;
    }

    /**
     * Create the default number of events using a metadata-first approach
     *
     * <pre>
     * Metadata-first approach:
     * First generate all combinations of metadata columns {i, ri, e, tf}
     * Then, generate all combinations of normalizers
     * Then, for each metadata column roll through each normalizer.
     * Then, for each field (alpha, numeric) apply appropriate data (fixed, random)
     * </pre>
     */
    public void createEvents() {
        createEvents(DEFAULT_EVENT_COUNT, DEFAULT_VALUES_PER_FIELD);
    }

    /**
     * Create the provided number of events using a metadata-first approach
     *
     * <pre>
     * Metadata-first approach:
     * First generate all combinations of metadata columns {i, ri, e, tf}
     * Then, generate all combinations of normalizers
     * Then, for each metadata column roll through each normalizer.
     * Then, for each field (alpha, numeric) apply appropriate data (fixed, random)
     * </pre>
     *
     * @param eventCount
     *            the number of events to generate
     */
    public void createEvents(int eventCount) {
        createEvents(eventCount, DEFAULT_VALUES_PER_FIELD);
    }

    /**
     * Creates the provided number of events using a metadata-first approach
     *
     * <pre>
     * Metadata-first approach:
     * First generate all combinations of metadata columns {i, ri, e, tf}
     * Then, generate all combinations of normalizers
     * Then, for each metadata column roll through each normalizer.
     * Then, for each field (alpha, numeric) apply appropriate data (fixed, random)
     * </pre>
     *
     * @param eventCount
     *            the number of events to generate
     * @param valuesPerField
     *            the number of distinct values generated for each field
     */
    public void createEvents(int eventCount, int valuesPerField) {
        Preconditions.checkArgument(eventCount > 0, "Event count must be positive");
        Preconditions.checkArgument(valuesPerField > 0, "Values per field must be positive");
        // generation appends to the field space, so a second call would duplicate every field name and leave plan() disagreeing with getFieldMetadata()
        Preconditions.checkState(fieldMetadata.isEmpty(), "Events have already been created");
        // logged so a failing run can be replayed via IngestMetadataBuilder.setSeed(long)
        log.info("generating values with seed {}", seed);
        List<List<MetadataColumn>> metadataColumns = Combination.getAllCombinations(baseMetadataColumns);
        int enabledFieldTypes = enabledFieldTypes();
        log.info("creating {} field metadata entries from {} metadata column combinations, {} normalizers and {} field name types", plan(),
                        metadataColumns.size(), baseNormalizers.size(), enabledFieldTypes);

        int eventsPerFieldSpace = eventCount / enabledFieldTypes;

        if (alphabeticFieldsEnabled) {
            FieldNameGenerator alphaNameGenerator = AlphabeticFieldNameGenerator.create(random);
            fieldMetadata.addAll(
                            createMetadataForFields(metadataColumns, baseNormalizers, alphaNameGenerator, eventCount, eventsPerFieldSpace, valuesPerField));
        }

        if (numericFieldsEnabled) {
            FieldNameGenerator numericNameGenerator = NumericFieldNameGenerator.create(random);
            fieldMetadata.addAll(
                            createMetadataForFields(metadataColumns, baseNormalizers, numericNameGenerator, eventCount, eventsPerFieldSpace, valuesPerField));
        }

        // always add the ID field
        fieldMetadata.add(createIDFieldMetadata(eventCount));

        for (FieldMetadata metadata : fieldMetadata) {
            int eventIdCount = metadata.getEventIds().size();
            log.info("{} count: {}", metadata.getFieldName(), eventIdCount);
        }

        this.eventCount = eventCount;
    }

    private List<FieldMetadata> createMetadataForFields(List<List<MetadataColumn>> metadataColumns, List<Type<?>> baseNormalizers,
                    FieldNameGenerator fieldNameGenerator, int eventCount, int eventsPerFieldSpace, int valuesPerField) {
        // 2x for offsets to ensure full coverage
        fieldNameGenerator.generate(metadataColumns.size() * baseNormalizers.size() * 2);
        Iterator<String> fieldNameIterator = new InfiniteIterator<>(fieldNameGenerator.getFieldNames());

        int maxMods = Math.max(fieldNameGenerator.getFieldNames().size(), eventsPerFieldSpace) / 2;
        // we are going to strip off the leading 1, so generate one more
        List<Integer> modCounts = SequentialEventIdGenerator.create().generateCount(maxMods + 1);
        // start with value 2
        modCounts = modCounts.subList(1, modCounts.size());
        Iterator<Integer> modCountIterator = new InfiniteIterator<>(modCounts);

        List<FieldMetadata> metadata = new ArrayList<>();
        for (List<MetadataColumn> metadataColumn : metadataColumns) {
            for (Type<?> normalizer : baseNormalizers) {
                boolean isContentCombo = metadataColumn.contains(TF);
                if (isContentCombo && !(normalizer instanceof LcNoDiacriticsType)) {
                    // phrases are text-only; skip pairings a phrase generator cannot service
                    continue;
                }

                int modCount = modCountIterator.next();
                for (int offset = 0; offset < FIELDS_PER_PAIRING; offset++) {
                    FieldMetadata field = new FieldMetadata(fieldNameIterator.next());
                    field.setFieldNumeric(fieldNameGenerator.isNumeric());
                    field.setMetadataColumns(metadataColumn);
                    field.setDatatypes(baseDatatypes);
                    field.setNormalizers(List.of(normalizer));

                    ValueGenerator<?> valueGenerator = isContentCombo ? PhraseGenerator.create(random) : getValueGeneratorForType(normalizer);
                    field.setValueGenerator(valueGenerator);
                    field.setEventIdGenerator(ModuloEventIdGenerator.create(modCount));

                    field.setOffset(offset);
                    field.populateValues(eventCount, valuesPerField);
                    metadata.add(field);
                }
            }
        }
        return metadata;
    }

    /**
     * The ID field should be present on every event
     *
     * @param eventCount
     *            the number of events requested
     * @return the FieldMetadata for the ID field
     */
    private FieldMetadata createIDFieldMetadata(int eventCount) {
        FieldMetadata metadata = new FieldMetadata(ID_FIELD_NAME);
        metadata.setDatatypes(baseDatatypes);
        metadata.setMetadataColumns(List.of(I, E));
        metadata.setNormalizers(List.of(new LcNoDiacriticsType()));
        metadata.setEventIdGenerator(SequentialEventIdGenerator.create());
        metadata.setValueGenerator(LinearNumberGenerator.create());
        metadata.setOffset(0);
        // the ID field needs exactly one distinct value per event, so request eventCount values directly rather than relying on populateValues to
        // implicitly cap an "unlimited" request by the event count
        metadata.populateValues(eventCount, eventCount);
        return metadata;
    }

    /**
     * Whether a value generator exists for the normalizer.
     * <p>
     * Checked by {@link IngestMetadataBuilder} when the normalizer is supplied, so an unsupported type is rejected at the call that introduced it rather than
     * partway through {@link #createEvents(int, int)}.
     *
     * @param type
     *            the normalizer
     * @return true if values can be generated for the normalizer
     */
    public static boolean isSupportedNormalizer(Type<?> type) {
        return type instanceof LcNoDiacriticsType || type instanceof NumberType || type instanceof NoOpType;
    }

    private ValueGenerator<?> getValueGeneratorForType(Type<?> type) {
        if (type instanceof LcNoDiacriticsType || type instanceof NoOpType) {
            return RandomAlphabeticGenerator.create(2, random);
        } else if (type instanceof NumberType) {
            return RandomNumericGenerator.create(random);
        } else {
            throw new IllegalStateException("Type not supported: " + type.getClass().getName());
        }
    }

    public List<MetadataColumn> getBaseMetadataColumns() {
        return baseMetadataColumns;
    }

    public List<Type<?>> getBaseNormalizers() {
        return baseNormalizers;
    }

    public boolean isAlphabeticFieldsEnabled() {
        return alphabeticFieldsEnabled;
    }

    public boolean isNumericFieldsEnabled() {
        return numericFieldsEnabled;
    }

    public List<FieldMetadata> getFieldMetadata() {
        return fieldMetadata;
    }

    public int getEventCount() {
        return eventCount;
    }

    public int getNumShards() {
        return numShards;
    }
}
