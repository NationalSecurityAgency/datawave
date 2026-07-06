package datawave.test.framework.generators.query;

import java.util.List;

/**
 * A common interface used with the AbstractQueryTest framework
 */
public interface QueryMetadataFactory {

    /**
     * Get the list of {@link QueryMetadata} that encapsulates the query, the plan, and the expected event ids
     *
     * @return a list of query metadata
     */
    List<QueryMetadata> getQueries();

}
