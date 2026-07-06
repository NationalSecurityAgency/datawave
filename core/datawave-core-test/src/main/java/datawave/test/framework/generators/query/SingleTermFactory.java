package datawave.test.framework.generators.query;

import java.util.ArrayList;
import java.util.List;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.QueryFieldMetadata;
import datawave.test.framework.generators.query.term.QueryTerm;

/**
 * An instance of {@link QueryMetadataFactory} that handles single indexed terms
 */
public class SingleTermFactory implements QueryMetadataFactory {

    private final QueryTerm term;
    private List<FieldMetadata> fieldMetadata;

    /**
     * Constructor that accepts {@link QueryFieldMetadata}
     */
    public SingleTermFactory(QueryTerm term) {
        this.term = term;
        if (term.isNegated()) {
            throw new IllegalStateException("Negated single terms are not supported");
        }
    }

    public void setFieldMetadata(List<FieldMetadata> fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
    }

    /**
     * Generate the list of {@link QueryMetadata} for single indexed terms
     *
     * @return a list of query metadata
     */
    @Override
    public List<QueryMetadata> getQueries() {
        List<QueryMetadata> metadata = new ArrayList<>();
        for (FieldMetadata field : fieldMetadata) {
            term.givenFieldMetadata(field);
            for (String value : term.valuesFor(field)) {
                term.givenValue(value);
                metadata.add(QueryMetadata.of(term.getQueryTerm(), term.getPlanTerm(), term.getEventIds()));
            }
        }
        return metadata;
    }
}
