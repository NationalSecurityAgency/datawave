package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.RI;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.test.framework.util.MetadataColumn;

public class IngestMetadataTest {

    @Test
    public void testSimplePlan() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        //  @formatter:off
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableAlphabeticFields()
                .build();
        //  @formatter:on

        // 3 metadata column combinations x 1 normalizer x 2 offsets x 1 field name type, plus the synthetic ID field
        int numUniqueFieldMetadata = metadata.plan();
        assertEquals(7, numUniqueFieldMetadata);
    }

    @Test
    public void testComplexPlan() {
        List<MetadataColumn> metadataColumns = List.of(I, RI, E, TF);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType(), new NumberType(), new NoOpType());

        //  @formatter:off
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableAlphabeticFields()
                .enableNumericFields()
                .build();
        //  @formatter:on

        int numUniqueFieldMetadata = metadata.plan();
        // TF combos paired with a non-text normalizer (NumberType, NoOpType) are skipped: a phrase generator doesn't apply to them.
        // 29 eligible pairings x 2 offsets x 2 field name types, plus the synthetic ID field
        assertEquals(117, numUniqueFieldMetadata);
    }

    /**
     * {@link IngestMetadata#plan()} exists so a configuration can be sized before it is generated, which is only useful if it agrees with what generation
     * actually produces. It previously counted neither the per-offset fields nor the synthetic ID field, under-reporting by more than half.
     */
    @Test
    public void testPlanMatchesGeneratedFieldCount() {
        //  @formatter:off
        List<List<MetadataColumn>> columnConfigurations = List.of(
                List.of(I, E),
                List.of(I, RI, E, TF),
                List.of(I, E, TF));
        //  @formatter:on

        for (List<MetadataColumn> columns : columnConfigurations) {
            //  @formatter:off
            IngestMetadata metadata = IngestMetadataBuilder.builder()
                    .setMetadataColumns(columns)
                    .addNormalizers(List.of(new LcNoDiacriticsType(), new NumberType()))
                    .enableAlphabeticFields()
                    .enableNumericFields()
                    .build();
            //  @formatter:on

            int planned = metadata.plan();
            metadata.createEvents(25);
            assertEquals(planned, metadata.getFieldMetadata().size(), "plan() disagrees with generated field count for columns " + columns);
        }
    }

    @Test
    public void testDefaultNumShards() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        IngestMetadata metadata = IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).addNormalizers(normalizers).enableAlphabeticFields()
                        .build();

        assertEquals(IngestMetadata.DEFAULT_NUM_SHARDS, metadata.getNumShards());
    }

    @Test
    public void testSimpleCreate() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        //  @formatter:off
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableAlphabeticFields()
                .build();
        //  @formatter:on
        metadata.createEvents();
    }

    /**
     * A failing statistical run is only actionable if it can be replayed, so the seed logged by {@link IngestMetadata#createEvents(int, int)} must fully
     * determine the generated values.
     */
    @Test
    public void testSameSeedProducesSameValues() {
        assertEquals(valuesForSeed(20260202L), valuesForSeed(20260202L));
    }

    /**
     * The companion to {@link #testSameSeedProducesSameValues()}: distinct seeds must actually explore distinct data, otherwise seeding would be reproducible
     * but useless.
     */
    @Test
    public void testDifferentSeedsProduceDifferentValues() {
        assertNotEquals(valuesForSeed(20260202L), valuesForSeed(20260203L));
    }

    /**
     * Generation appends to the field space, so a second call would duplicate every field name and leave {@link IngestMetadata#plan()} reporting half of what
     * {@link IngestMetadata#getFieldMetadata()} holds. A duplicated field surfaces far from its cause, as a statistical mismatch in an unrelated query.
     */
    @Test
    public void testCreateEventsRejectsASecondCall() {
        //  @formatter:off
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(List.of(I, E))
                .addNormalizers(List.of(new LcNoDiacriticsType()))
                .enableAlphabeticFields()
                .build();
        //  @formatter:on
        metadata.createEvents();
        int fieldCount = metadata.getFieldMetadata().size();

        Exception e = assertThrows(IllegalStateException.class, metadata::createEvents);
        assertEquals("Events have already been created", e.getMessage());
        assertEquals(fieldCount, metadata.getFieldMetadata().size());
    }

    /**
     * A normalizer with no value generator behind it used to fail partway through generation, after some field metadata had already been built. Rejecting it
     * where it is supplied puts the error at the call the caller can act on.
     */
    @Test
    public void testUnsupportedNormalizerIsRejectedWhenSupplied() {
        IngestMetadataBuilder builder = IngestMetadataBuilder.builder();
        Exception e = assertThrows(IllegalArgumentException.class, () -> builder.addNormalizer(new DateType()));
        assertEquals("normalizer not supported: datawave.data.type.DateType", e.getMessage());
    }

    private List<String> valuesForSeed(long seed) {
        //  @formatter:off
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(List.of(I, E, TF))
                .addNormalizers(List.of(new LcNoDiacriticsType(), new NumberType()))
                .enableAlphabeticFields()
                .enableNumericFields()
                .setSeed(seed)
                .build();
        //  @formatter:on
        metadata.createEvents(25);

        List<String> values = new ArrayList<>();
        for (FieldMetadata field : metadata.getFieldMetadata()) {
            values.addAll(field.getValues());
        }
        return values;
    }
}
