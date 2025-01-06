package datawave.webservice.query.result.event;

import datawave.microservice.query.Query;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.response.objects.KeyBase;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.FacetQueryResponseBase;

public abstract class ResponseObjectFactory {

    public abstract EventBase getEvent();

    public abstract FieldBase getField();

    public abstract EventQueryResponseBase getEventQueryResponse();

    public abstract CacheableQueryRow getCacheableQueryRow();

    public abstract EdgeBase getEdge();

    public abstract EdgeQueryResponseBase getEdgeQueryResponse();

    public abstract FacetQueryResponseBase getFacetQueryResponse();

    public abstract FacetsBase getFacets();

    public abstract FieldCardinalityBase getFieldCardinality();

    /**
     * Get a KeyBase implementation. Note that this is currently used by Entry objects which are used in the LookupResponse. If a specific implementation is
     * provided here, then a javax.ws.rs.ext.Provider must be created which implements {@code ContextResolver<JAXBContext>}. Therein a resolver for a
     * LookupResponse needs to include the provided implementation within a jaxb context to ensure appropriate serialization.
     *
     * @return the keybase
     */
    public abstract KeyBase getKey();

    public abstract AuthorizationsListBase getAuthorizationsList();

    public abstract Query getQueryImpl();

    public abstract DataDictionaryBase getDataDictionary();

    public abstract FieldsBase getFields();

    public abstract DescriptionBase getDescription();

    public abstract MetadataFieldBase getMetadataField();

}
