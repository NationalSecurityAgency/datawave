package datawave.test.framework.generators.query.term;

import java.util.ArrayList;
import java.util.List;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.MetadataColumn;

/**
 * Shared fixtures for concrete {@link QueryTerm} tests.
 * <p>
 * {@link #apply(QueryTerm, FieldMetadata)} records the values exposed by each evaluation callback.
 */
abstract class AbstractQueryTermTest {

    /**
     * One application of a term: the query text, plan text and event ids it produced.
     */
    protected static class Evaluation {

        private final String query;
        private final String plan;
        private final List<Integer> ids;

        Evaluation(String query, String plan, List<Integer> ids) {
            this.query = query;
            this.plan = plan;
            this.ids = ids;
        }

        public String getQuery() {
            return query;
        }

        public String getPlan() {
            return plan;
        }

        public List<Integer> getIds() {
            return ids;
        }
    }

    /**
     * Apply a term to a field, collecting one {@link Evaluation} per application.
     *
     * @param term
     *            the term to apply
     * @param field
     *            the field to apply it to
     * @return the resulting evaluations, in application order
     */
    protected List<Evaluation> apply(QueryTerm term, FieldMetadata field) {
        List<Evaluation> evaluations = new ArrayList<>();
        term.forEachEvaluation(field, () -> evaluations.add(new Evaluation(term.getQueryTerm(), term.getPlanTerm(), term.getEventIds())));
        return evaluations;
    }

    /**
     * A field with the given values and events, carrying a single {@link LcNoDiacriticsType} normalizer.
     */
    protected FieldMetadata field(List<String> values, List<Integer> eventIds, MetadataColumn... columns) {
        return field(values, eventIds, List.of(new LcNoDiacriticsType()), columns);
    }

    protected FieldMetadata field(List<String> values, List<Integer> eventIds, List<Type<?>> normalizers, MetadataColumn... columns) {
        FieldMetadata field = new FieldMetadata("FIELD");
        field.setMetadataColumns(List.of(columns));
        field.setNormalizers(normalizers);
        field.setEventIds(eventIds);
        field.setValues(values);
        return field;
    }
}
