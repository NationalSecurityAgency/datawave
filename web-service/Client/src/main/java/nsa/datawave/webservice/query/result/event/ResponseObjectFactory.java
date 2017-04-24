package nsa.datawave.webservice.query.result.event;

import nsa.datawave.user.AuthorizationsListBase;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRow;
import nsa.datawave.webservice.query.result.EdgeQueryResponseBase;
import nsa.datawave.webservice.query.result.edge.EdgeBase;
import nsa.datawave.webservice.query.result.metadata.MetadataFieldBase;
import nsa.datawave.webservice.response.objects.KeyBase;
import nsa.datawave.webservice.result.EventQueryResponseBase;
import nsa.datawave.webservice.result.FacetQueryResponseBase;
import nsa.datawave.webservice.results.datadictionary.DataDictionaryBase;
import nsa.datawave.webservice.results.datadictionary.DescriptionBase;
import nsa.datawave.webservice.results.datadictionary.FieldsBase;

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
     * provided here, then a javax.ws.rs.ext.Provider must be created which implements ContextResolver<JAXBContext>. Therein a resolver for a LookupResponse
     * needs to include the provided implementation within a jaxb context to ensure appropriate serialization.
     * 
     * @return
     */
    public abstract KeyBase getKey();
    
    public abstract AuthorizationsListBase getAuthorizationsList();
    
    public abstract Query getQueryImpl();
    
    public abstract DataDictionaryBase getDataDictionary();
    
    public abstract FieldsBase getFields();
    
    public abstract DescriptionBase getDescription();
    
    public abstract MetadataFieldBase getMetadataField();
    
}
