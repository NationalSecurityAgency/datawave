package datawave.webservice.annotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.microservice.query.Query;
import datawave.query.QueryParameters;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;

/**
 * A service for performing lookupUUID to find the internal id for common identifiers. Implements a small, simplified subset of the functionality of
 * LookupUUIDUtil
 */
public class LookupUUIDService {

    private static final Logger log = LoggerFactory.getLogger(LookupUUIDService.class);

    private final LookupUUIDServiceConfig config;
    private final AccumuloClient client;
    private final ResponseObjectFactory responseObjectFactory;
    private final ShardQueryLogic lookupUUIDQueryLogic;
    private final Set<Authorizations> authorizations;

    /**
     *
     * @param config
     *            the configuration
     * @param client
     *            an accumulo client to use for lookups
     * @param authorizations
     *            authorizations to use for lookups
     * @param responseObjectFactory
     *            the factory we'll use for query generation
     * @param lookupUUIDQueryLogic
     *            the underlying query logic we'll call to perform the lookups.
     */
    public LookupUUIDService(LookupUUIDServiceConfig config, AccumuloClient client, Set<Authorizations> authorizations,
                    ResponseObjectFactory responseObjectFactory, ShardQueryLogic lookupUUIDQueryLogic) {
        this.config = config;
        this.client = client;
        this.authorizations = authorizations;
        this.responseObjectFactory = responseObjectFactory;
        this.lookupUUIDQueryLogic = lookupUUIDQueryLogic;
    }

    /**
     * Executes a lookupUUID query for the specified idType and it and returns the item's internal identifiers (e.g., shard, datatype, uid) packaged in a
     * Metadata object.
     *
     * @param idType
     *            the type of id to query.
     * @param id
     *            the id value to query.
     * @return a list of zero to many Metadata objects with the internal shard, datatype, uid and table name of the identifier(s) provided. The list will be
     *         empty if no identifier could be found using the authorizations and query logic employed by this class.
     * @throws QueryException
     *             if exceptions are encountered performing the lookup.
     */
    public List<Metadata> executeLookupUUIDQuery(String idType, String id) throws QueryException {
        final String query = String.format("%s:%s", idType, id);
        final Map<String,String> queryParameters = new HashMap<>();
        queryParameters.put(QueryParameters.QUERY_SYNTAX, "LUCENE-UUID");
        // TODO: setup other parameters?

        Query settings = responseObjectFactory.getQueryImpl();
        settings.setBeginDate(config.getBeginAsDate());
        settings.setEndDate(config.getEndAsDate());
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(getAuthorizationString(authorizations));
        settings.setQuery(query);
        settings.setParameters(queryParameters);
        settings.setId(UUID.randomUUID());

        try {
            final GenericQueryConfiguration config = lookupUUIDQueryLogic.initialize(client, settings, authorizations);
            lookupUUIDQueryLogic.setupQuery(config);
        } catch (Exception e) {
            throw new QueryException("Exception looking up internal id for idType: " + idType + " id: " + id, e);
        }

        DocumentTransformer transformer = (DocumentTransformer) (lookupUUIDQueryLogic.getTransformer(settings));
        TransformIterator<?,? extends EventBase<?,?>> iter = new DatawaveTransformIterator<>(lookupUUIDQueryLogic.iterator(), transformer);
        List<Metadata> metadataList = new ArrayList<>();
        while (iter.hasNext()) {
            EventBase<?,?> e = iter.next();
            datawave.webservice.query.result.event.Metadata eventMetadata = e.getMetadata();
            if (log.isDebugEnabled()) {

                String metadataMessage = String.format("%s/%s/%s [%s]", eventMetadata.getRow(), eventMetadata.getDataType(), eventMetadata.getInternalId(),
                                eventMetadata.getTable());
                log.debug("Found metadata {} for idType {}, id {}", metadataMessage, idType, id);
            }
            metadataList.add(new Metadata(eventMetadata));
        }
        return metadataList;
    }

    private static String getAuthorizationString(Set<Authorizations> authorizations) {
        Authorizations auths = authorizations.iterator().next();
        return auths.serialize();
    }
}
