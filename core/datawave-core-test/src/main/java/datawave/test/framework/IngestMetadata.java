package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.DateType;
import datawave.data.type.IpAddressType;
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
     * The field name of the synthetic, per-event unique identifier field. Callers that need to build queries against it should use
     * {@link datawave.test.framework.generators.query.QueryGenerator#singleTermId} rather than looking it up by this name directly.
     */
    public static final String ID_FIELD_NAME = "ID";

    private final boolean alphabeticFieldsEnabled;
    private final boolean numericFieldsEnabled;
    private final int numShards;
    private final List<MetadataColumn> baseMetadataColumns;
    private final List<String> baseDatatypes = List.of("datatype-a");
    private final List<Type<?>> baseNormalizers;

    private final List<FieldMetadata> fieldMetadata = new ArrayList<>();

    private final List<Multimap<String,Object>> events = new ArrayList<>();

    private int eventCount = DEFAULT_EVENT_COUNT;
    private int valuesPerField = DEFAULT_VALUES_PER_FIELD;

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
     */
    public IngestMetadata(List<MetadataColumn> baseMetadataColumns, List<Type<?>> baseNormalizers, boolean alphabeticFieldsEnabled,
                    boolean numericFieldsEnabled, int numShards) {
        this.baseMetadataColumns = baseMetadataColumns;
        this.baseNormalizers = baseNormalizers;
        this.alphabeticFieldsEnabled = alphabeticFieldsEnabled;
        this.numericFieldsEnabled = numericFieldsEnabled;
        this.numShards = numShards;
    }

    /**
     * Plan the number of unique {@link FieldMetadata} entries given the input metadata columns, normalizers, and field types.
     *
     * @return the number of unique {@link FieldMetadata} entries
     */
    public int plan() {
        List<List<MetadataColumn>> metadataColumns = Combination.getAllCombinations(baseMetadataColumns);
        log.info("planning {} metadata columns", metadataColumns.size());

        // normalizers do not get combinatoric
        log.info("planning {} normalizers", baseNormalizers.size());

        int enabledFieldTypes = 0;
        if (numericFieldsEnabled) {
            enabledFieldTypes++;
        }
        if (alphabeticFieldsEnabled) {
            enabledFieldTypes++;
        }
        log.info("planning {} field name types", enabledFieldTypes);

        // metadata * normalizers * 2 fields, excluding TF combos paired with a non-text normalizer
        int totalCombos = countEligiblePairings(metadataColumns) * enabledFieldTypes;
        log.info("planned {} combinations", totalCombos);

        return totalCombos;
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
     */
    public void createEvents(int eventCount, int valuesPerField) {
        Preconditions.checkArgument(eventCount > 0, "Event count must be positive");
        this.valuesPerField = valuesPerField;
        List<List<MetadataColumn>> metadataColumns = Combination.getAllCombinations(baseMetadataColumns);
        log.info("creating {} metadata columns", metadataColumns.size());
        log.info("creating {} normalizers", baseNormalizers.size());

        int enabledFieldTypes = 0;
        if (numericFieldsEnabled) {
            enabledFieldTypes++;
        }
        if (alphabeticFieldsEnabled) {
            enabledFieldTypes++;
        }
        int totalCombos = countEligiblePairings(metadataColumns) * enabledFieldTypes;
        log.info("created {} combinations", totalCombos);

        int eventsPerFieldSpace = eventCount / enabledFieldTypes;

        if (alphabeticFieldsEnabled) {
            FieldNameGenerator alphaNameGenerator = AlphabeticFieldNameGenerator.create();
            fieldMetadata.addAll(createMetadataForFields(metadataColumns, baseNormalizers, alphaNameGenerator, eventCount, eventsPerFieldSpace));
        }

        if (numericFieldsEnabled) {
            FieldNameGenerator numericNameGenerator = NumericFieldNameGenerator.create();
            fieldMetadata.addAll(createMetadataForFields(metadataColumns, baseNormalizers, numericNameGenerator, eventCount, eventsPerFieldSpace));
        }

        // always add the ID field
        fieldMetadata.add(createIDFieldMetadata(eventCount));

        for (FieldMetadata metadata : fieldMetadata) {
            int eventIdCount = metadata.getEventIds().size();
            log.info("{} count: {}", metadata.getFieldName(), eventIdCount);
        }

        for (int i = 0; i < eventCount; i++) {
            Multimap<String,Object> event = ArrayListMultimap.create();
            for (FieldMetadata metadata : fieldMetadata) {
                if (metadata.getEventIds().contains(i)) {
                    String field = metadata.getFieldName();
                    Object value = metadata.getValueForEventId(i);
                    event.put(field, value);
                }
            }

            if (event.size() > 1) {
                events.add(event);
            }
        }

        log.info("{} requested events, {} were generated", eventCount, events.size());
        this.eventCount = eventCount;
    }

    private List<FieldMetadata> createMetadataForFields(List<List<MetadataColumn>> metadataColumns, List<Type<?>> baseNormalizers,
                    FieldNameGenerator fieldNameGenerator, int eventCount, int eventsPerFieldSpace) {
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
                for (int offset = 0; offset < 2; offset++) {
                    FieldMetadata fieldMetadata = new FieldMetadata(fieldNameIterator.next());
                    fieldMetadata.setFieldNumeric(fieldNameGenerator.isNumeric());
                    fieldMetadata.setMetadataColumns(metadataColumn);
                    fieldMetadata.setDatatypes(baseDatatypes);
                    fieldMetadata.setNormalizers(List.of(normalizer));

                    ValueGenerator<?> valueGenerator = isContentCombo ? PhraseGenerator.create() : getValueGeneratorForType(normalizer);
                    fieldMetadata.setValueGenerator(valueGenerator);
                    fieldMetadata.setEventIdGenerator(ModuloEventIdGenerator.create(modCount));

                    fieldMetadata.setOffset(offset);
                    fieldMetadata.populateValues(eventCount, valuesPerField);
                    metadata.add(fieldMetadata);
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

    private ValueGenerator<?> getValueGeneratorForType(Type<?> type) {
        if (type instanceof LcNoDiacriticsType) {
            return RandomAlphabeticGenerator.create(2);
        } else if (type instanceof IpAddressType) {
            throw new IllegalStateException("IpAddressType not supported");
        } else if (type instanceof NumberType) {
            return RandomNumericGenerator.create();
        } else if (type instanceof DateType) {
            throw new IllegalStateException("DateType not supported");
        } else if (type instanceof NoOpType) {
            return RandomAlphabeticGenerator.create(2);
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

    public List<Multimap<String,Object>> getEvents() {
        return events;
    }

    public int getEventCount() {
        return eventCount;
    }

    public int getNumShards() {
        return numShards;
    }
}
