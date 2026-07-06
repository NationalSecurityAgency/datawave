package datawave.test.framework.generators.query;

import java.util.List;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.QueryFieldMetadata;
import datawave.test.framework.generators.query.term.QueryTerm;

/**
 * Singular entrypoint for all instances of {@link QueryMetadataFactory}
 * <p>
 * This class is the record of every supported {@code QueryMetadataFactory} x field-source combination. Each method wires a factory to a specific,
 * deliberately-chosen pairing of field sources ({@code indexed}, {@code indexOnly}, {@code eventOnly}); the {@link QueryTerm} implementations remain a
 * parameter so callers can exercise any term combination against a given shape.
 * <p>
 * Some combinations are intentionally not offered here, because they are not meaningful/executable DataWave queries:
 * <ul>
 * <li><b>Single event-only term</b> - an event-only field is not indexed, so a lone {@code FIELD == 'value'} against it has no indexed anchor.</li>
 * <li><b>Union with an event-only side</b> - each branch of an OR must independently resolve via the index; an event-only field cannot.</li>
 * <li><b>Intersection of two event-only sides</b> - neither side is indexed, so the intersection as a whole has no indexed anchor.</li>
 * <li><b>Filter functions (isNotNull/isNull) paired via {@link #intersectionIndexedTerms} or {@link #intersectionIndexOnlyTerms}</b> - both methods draw from a
 * field pool that may include index-only fields, which carry no event/document data. DataWave rejects filter functions targeting an index-only field
 * ("index-only fields are mixed with expressions that cannot be run against the index"). Filter functions must instead be paired with an event-bearing field
 * via {@link #intersectionIndexedAndEventOnly} or {@link #intersectionIndexOnlyAndEventOnly}.</li>
 * </ul>
 */
public class QueryGenerator {

    private final QueryFieldMetadata queryFieldMetadata;

    /**
     * Static entry point for the query generator
     *
     * @param fieldMetadata
     *            the list of field metadata
     * @return a new instance of {@link QueryGenerator}
     */
    public static QueryGenerator create(List<FieldMetadata> fieldMetadata) {
        return new QueryGenerator(fieldMetadata);
    }

    /**
     * Private constructor
     *
     * @param fieldMetadata
     *            the list of field metadata
     */
    private QueryGenerator(List<FieldMetadata> fieldMetadata) {
        this.queryFieldMetadata = QueryFieldMetadata.of(fieldMetadata);
    }

    /**
     * Access the underlying {@link QueryFieldMetadata} so callers can wire up {@link BinaryTermFactory} implementations against arbitrary field sources
     * (indexed, index-only, event-only) not covered by the methods below.
     *
     * @return the query field metadata
     */
    public QueryFieldMetadata fieldMetadata() {
        return queryFieldMetadata;
    }

    /**
     * Create a {@link QueryMetadataFactory} for a single term against indexed fields (includes index-only fields)
     *
     * @return a single term factory
     */
    public SingleTermFactory singleTermIndexed(QueryTerm term) {
        SingleTermFactory factory = new SingleTermFactory(term);
        factory.setFieldMetadata(queryFieldMetadata.getIndexed());
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for a single term against index-only fields
     *
     * @return a single term factory
     */
    public SingleTermFactory singleTermIndexOnly(QueryTerm term) {
        SingleTermFactory factory = new SingleTermFactory(term);
        factory.setFieldMetadata(queryFieldMetadata.getIndexOnly());
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for a single term against the synthetic ID field. The ID field is excluded from {@link #singleTermIndexed} and the
     * other field-source methods because its value count is defined to always equal the event count.
     *
     * @return a single term factory
     */
    public SingleTermFactory singleTermId(QueryTerm term) {
        SingleTermFactory factory = new SingleTermFactory(term);
        factory.setFieldMetadata(queryFieldMetadata.getId());
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for a single term against tokenized, index-only fields (fields with
     * {@link datawave.test.framework.util.MetadataColumn#TF} and {@link datawave.test.framework.util.MetadataColumn#I}, without
     * {@link datawave.test.framework.util.MetadataColumn#E}). Intended for {@code content:phrase(...)}-style terms, which can self-anchor via the field index
     * just like an equality term against an index-only field.
     *
     * @return a single term factory
     */
    public SingleTermFactory singleTermTokenizedIndexOnly(QueryTerm term) {
        SingleTermFactory factory = new SingleTermFactory(term);
        factory.setFieldMetadata(queryFieldMetadata.getTokenizedIndexOnly());
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for an intersection of an indexed term (the anchor) and a tokenized, event-only term (fields with
     * {@link datawave.test.framework.util.MetadataColumn#TF} and {@link datawave.test.framework.util.MetadataColumn#E}, without
     * {@link datawave.test.framework.util.MetadataColumn#I}). A tokenized event-only field has no index anchor of its own, so a {@code content:phrase(...)}
     * term against it must be paired with an indexed anchor elsewhere in the query, the same way filter functions are paired via
     * {@link #intersectionIndexedAndEventOnly}.
     *
     * @param leftTerm
     *            the left, indexed {@link QueryTerm}
     * @param rightTerm
     *            the right, tokenized event-only {@link QueryTerm}
     * @return an intersection factory anchored on an indexed field, paired with a tokenized event-only field
     */
    public IntersectionFactory intersectionIndexedAndTokenizedEventOnly(QueryTerm leftTerm, QueryTerm rightTerm) {
        IntersectionFactory factory = new IntersectionFactory(leftTerm, rightTerm);
        factory.setLeftMetadata(queryFieldMetadata.getIndexed());
        factory.setRightMetadata(queryFieldMetadata.getTokenizedEventOnly());
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for unions of indexed terms
     *
     * @param leftTerm
     *            the left {@link QueryTerm}
     * @param rightTerm
     *            the right {@link QueryTerm}
     * @return a union indexed term factory
     */
    public UnionFactory unionIndexedTerms(QueryTerm leftTerm, QueryTerm rightTerm) {
        UnionFactory union = new UnionFactory(leftTerm, rightTerm);
        // both sides pull from the same indexed field list, so the factory treats this as self-composition
        List<FieldMetadata> indexed = queryFieldMetadata.getIndexed();
        union.setLeftMetadata(indexed);
        union.setRightMetadata(indexed);
        return union;
    }

    /**
     * Create a {@link QueryMetadataFactory} for unions of index-only terms
     *
     * @param leftTerm
     *            the left {@link QueryTerm}
     * @param rightTerm
     *            the right {@link QueryTerm}
     * @return a union index-only term factory
     */
    public UnionFactory unionIndexOnlyTerms(QueryTerm leftTerm, QueryTerm rightTerm) {
        UnionFactory union = new UnionFactory(leftTerm, rightTerm);
        // both sides pull from the same index-only field list, so the factory treats this as self-composition
        List<FieldMetadata> indexOnly = queryFieldMetadata.getIndexOnly();
        union.setLeftMetadata(indexOnly);
        union.setRightMetadata(indexOnly);
        return union;
    }

    /**
     * Create a {@link QueryMetadataFactory} for intersections of indexed terms
     *
     * @param leftTerm
     *            the left {@link QueryTerm}
     * @param rightTerm
     *            the right {@link QueryTerm}
     * @return an intersection indexed term factory
     */
    public IntersectionFactory intersectionIndexedTerms(QueryTerm leftTerm, QueryTerm rightTerm) {
        IntersectionFactory factory = new IntersectionFactory(leftTerm, rightTerm);
        // both sides pull from the same indexed field list, so the factory treats this as self-composition
        List<FieldMetadata> indexed = queryFieldMetadata.getIndexed();
        factory.setLeftMetadata(indexed);
        factory.setRightMetadata(indexed);
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for intersections of index-only terms
     *
     * @param leftTerm
     *            the left {@link QueryTerm}
     * @param rightTerm
     *            the right {@link QueryTerm}
     * @return an intersection index-only term factory
     */
    public IntersectionFactory intersectionIndexOnlyTerms(QueryTerm leftTerm, QueryTerm rightTerm) {
        IntersectionFactory factory = new IntersectionFactory(leftTerm, rightTerm);
        // both sides pull from the same index-only field list, so the factory treats this as self-composition
        List<FieldMetadata> indexOnly = queryFieldMetadata.getIndexOnly();
        factory.setLeftMetadata(indexOnly);
        factory.setRightMetadata(indexOnly);
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for an intersection of an indexed term (the anchor) and an event-only term
     *
     * @param leftTerm
     *            the left, indexed {@link QueryTerm}
     * @param rightTerm
     *            the right, event-only {@link QueryTerm}
     * @return an intersection factory anchored on an indexed field, paired with an event-only field
     */
    public IntersectionFactory intersectionIndexedAndEventOnly(QueryTerm leftTerm, QueryTerm rightTerm) {
        IntersectionFactory factory = new IntersectionFactory(leftTerm, rightTerm);
        factory.setLeftMetadata(queryFieldMetadata.getIndexed());
        factory.setRightMetadata(queryFieldMetadata.getEventOnly());
        return factory;
    }

    /**
     * Create a {@link QueryMetadataFactory} for an intersection of an index-only term (the anchor) and an event-only term
     *
     * @param leftTerm
     *            the left, index-only {@link QueryTerm}
     * @param rightTerm
     *            the right, event-only {@link QueryTerm}
     * @return an intersection factory anchored on an index-only field, paired with an event-only field
     */
    public IntersectionFactory intersectionIndexOnlyAndEventOnly(QueryTerm leftTerm, QueryTerm rightTerm) {
        IntersectionFactory factory = new IntersectionFactory(leftTerm, rightTerm);
        factory.setLeftMetadata(queryFieldMetadata.getIndexOnly());
        factory.setRightMetadata(queryFieldMetadata.getEventOnly());
        return factory;
    }

}
