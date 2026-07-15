package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.RI;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.test.framework.generators.query.QueryGenerator;
import datawave.test.framework.generators.query.QueryMetadata;
import datawave.test.framework.generators.query.term.EqTerm;
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
        //  @formater:on

        int numUniqueFieldMetadata = metadata.plan();
        assertEquals(3, numUniqueFieldMetadata);
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
        //  @formater:on

        int numUniqueFieldMetadata = metadata.plan();
        // TF combos paired with a non-text normalizer (NumberType, NoOpType) are skipped: a phrase generator doesn't apply to them.
        assertEquals(58, numUniqueFieldMetadata);
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
        //  @formater:on
        metadata.createEvents();
    }

    /**
     * The number of queries generated for a regular content field is a function of the ingest configuration (metadata columns, normalizers, field types,
     * valuesPerField), not of how many events were actually requested. Changing eventCount must not change the query count.
     * <p>
     * {@link QueryGenerator#singleTermIndexed} excludes the synthetic ID field, which is defined to carry exactly one distinct value per event (its query
     * count is expected to scale with eventCount by design; see {@link QueryGenerator#singleTermId}).
     */
    @Test
    public void testQueryCountIsIndependentOfEventCount() {
        List<MetadataColumn> metadataColumns = List.of(I, E);
        List<Type<?>> normalizers = List.of(new LcNoDiacriticsType());

        IngestMetadata small = IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).addNormalizers(normalizers).enableAlphabeticFields()
                        .build();
        small.createEvents(10);

        IngestMetadata large = IngestMetadataBuilder.builder().setMetadataColumns(metadataColumns).addNormalizers(normalizers).enableAlphabeticFields()
                        .build();
        large.createEvents(200);

        List<QueryMetadata> smallQueries = QueryGenerator.create(small.getFieldMetadata()).singleTermIndexed(new EqTerm()).getQueries();
        List<QueryMetadata> largeQueries = QueryGenerator.create(large.getFieldMetadata()).singleTermIndexed(new EqTerm()).getQueries();

        assertEquals(smallQueries.size(), largeQueries.size());
    }
}