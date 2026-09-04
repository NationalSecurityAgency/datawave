package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.test.framework.util.MetadataColumn;

public class IngestMetadataBuilderTest {

    @Test
    public void testInvalidMetadataColumns() {
        Exception e1 = assertThrows(IllegalStateException.class, () -> IngestMetadataBuilder.builder().build());
        assertEquals("metadataColumns must be set", e1.getMessage());

        Exception e2 = assertThrows(NullPointerException.class, () -> IngestMetadataBuilder.builder().setMetadataColumns(null).build());
        assertEquals("metadataColumns cannot be null", e2.getMessage());

        // rejecting the argument itself, rather than the builder's state, so this is an IllegalArgumentException
        Exception e3 = assertThrows(IllegalArgumentException.class, () -> IngestMetadataBuilder.builder().setMetadataColumns(Collections.emptyList()).build());
        assertEquals("metadataColumns cannot be empty", e3.getMessage());
    }

    @Test
    public void testInvalidNormalizer() {
        List<MetadataColumn> metadataColumns = List.of(I, E);

        Exception e1 = assertThrows(IllegalStateException.class, () -> IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).build());
        assertEquals("normalizers must be set", e1.getMessage());

        Exception e2 = assertThrows(NullPointerException.class,
                        () -> IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).addNormalizer(null).build());
        assertEquals("normalizer cannot be null", e2.getMessage());
    }

    @Test
    public void testInvalidNormalizers() {
        List<MetadataColumn> metadataColumns = List.of(I, E);

        Exception e1 = assertThrows(NullPointerException.class,
                        () -> IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).addNormalizers(null).build());
        assertEquals("normalizers cannot be null", e1.getMessage());

        // rejecting the argument itself, rather than the builder's state, so this is an IllegalArgumentException
        Exception e2 = assertThrows(IllegalArgumentException.class,
                        () -> IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).addNormalizers(Collections.emptyList()).build());
        assertEquals("normalizers cannot be empty", e2.getMessage());
    }

    @Test
    public void testInvalidFieldState() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        //  @formatter:off
        Exception e = assertThrows(IllegalStateException.class, () -> IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .build());
        //  @formatter:on
        assertEquals("alphabetic or numeric fields must be enabled", e.getMessage());
    }

    @Test
    public void testInvalidNumShards() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        Exception e1 = assertThrows(IllegalArgumentException.class, () -> IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns)
                        .addNormalizers(normalizers).enableAlphabeticFields().setNumShards(0).build());
        assertEquals("numShards must be greater than 0", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns)
                        .addNormalizers(normalizers).enableAlphabeticFields().setNumShards(-1).build());
        assertEquals("numShards must be greater than 0", e2.getMessage());
    }

    @Test
    public void testCustomNumShards() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        //  @formatter:off
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableAlphabeticFields()
                .setNumShards(5)
                .build();
        //  @formatter:on

        assertEquals(5, metadata.getNumShards());
    }

    @Test
    public void testValidBuild() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        //  @formatter:off
        //  only alphabetic fields enabled
        IngestMetadata metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableAlphabeticFields()
                .build();
        //  @formatter:on

        assertEquals(metadataColumns, metadata.getBaseMetadataColumns());
        assertEquals(normalizers, metadata.getBaseNormalizers());

        //  @formatter:off
        //  only numeric fields enabled
        metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableNumericFields()
                .build();
        //  @formatter:on

        assertEquals(metadataColumns, metadata.getBaseMetadataColumns());
        assertEquals(normalizers, metadata.getBaseNormalizers());

        //  @formatter:off
        //  both alphabetic and numeric fields enabled
        metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(metadataColumns)
                .addNormalizers(normalizers)
                .enableAlphabeticFields()
                .enableNumericFields()
                .build();
        //  @formatter:on

        assertEquals(metadataColumns, metadata.getBaseMetadataColumns());
        assertEquals(normalizers, metadata.getBaseNormalizers());
    }

    /**
     * A builder yields one instance. Were reuse allowed, the seed drawn at builder construction would carry over and the second build would generate data
     * identical to the first.
     */
    @Test
    public void testBuilderIsSingleUse() {
        //  @formatter:off
        IngestMetadataBuilder builder = IngestMetadataBuilder.builder()
                .setMetadataColumns(List.of(I, E))
                .addNormalizer(new LcNoDiacriticsType())
                .enableAlphabeticFields();
        //  @formatter:on

        builder.build();

        Exception e = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("build() has already been called on this builder", e.getMessage());
    }

    /**
     * A failed build leaves the builder usable, so a caller can correct the configuration and try again.
     */
    @Test
    public void testFailedBuildDoesNotConsumeTheBuilder() {
        IngestMetadataBuilder builder = IngestMetadataBuilder.builder().setMetadataColumns(List.of(I, E)).addNormalizer(new LcNoDiacriticsType());

        assertThrows(IllegalStateException.class, builder::build);

        // 3 metadata column combinations x 1 normalizer x 2 offsets x 1 field name type, plus the synthetic ID field
        assertEquals(7, builder.enableAlphabeticFields().build().plan());
    }
}
