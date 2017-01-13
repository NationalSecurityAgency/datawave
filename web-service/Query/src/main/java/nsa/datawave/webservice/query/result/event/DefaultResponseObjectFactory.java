package nsa.datawave.webservice.query.result.event;

import nsa.datawave.user.AuthorizationsListBase;
import nsa.datawave.user.DefaultAuthorizationsList;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRow;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRowImpl;
import nsa.datawave.webservice.query.result.EdgeQueryResponseBase;
import nsa.datawave.webservice.query.result.edge.DefaultEdge;
import nsa.datawave.webservice.query.result.edge.EdgeBase;
import nsa.datawave.webservice.query.result.metadata.DefaultMetadataField;
import nsa.datawave.webservice.query.result.metadata.MetadataFieldBase;
import nsa.datawave.webservice.response.objects.DefaultKey;
import nsa.datawave.webservice.response.objects.KeyBase;
import nsa.datawave.webservice.result.DefaultEdgeQueryResponse;
import nsa.datawave.webservice.result.DefaultEventQueryResponse;
import nsa.datawave.webservice.result.EventQueryResponseBase;
import nsa.datawave.webservice.result.FacetQueryResponse;
import nsa.datawave.webservice.result.FacetQueryResponseBase;
import nsa.datawave.webservice.results.datadictionary.DataDictionaryBase;
import nsa.datawave.webservice.results.datadictionary.DefaultDataDictionary;
import nsa.datawave.webservice.results.datadictionary.DefaultDescription;
import nsa.datawave.webservice.results.datadictionary.DefaultFields;
import nsa.datawave.webservice.results.datadictionary.DescriptionBase;
import nsa.datawave.webservice.results.datadictionary.FieldsBase;

public class DefaultResponseObjectFactory extends ResponseObjectFactory {
    @Override
    public EventBase getEvent() {
        return new DefaultEvent();
    }
    
    @Override
    public FieldBase getField() {
        return new DefaultField();
    }
    
    @Override
    public EventQueryResponseBase getEventQueryResponse() {
        return new DefaultEventQueryResponse();
    }
    
    @Override
    public CacheableQueryRow getCacheableQueryRow() {
        return new CacheableQueryRowImpl();
    }
    
    @Override
    public EdgeBase getEdge() {
        return new DefaultEdge();
    }
    
    @Override
    public EdgeQueryResponseBase getEdgeQueryResponse() {
        return new DefaultEdgeQueryResponse();
    }
    
    @Override
    public FacetQueryResponseBase getFacetQueryResponse() {
        return new FacetQueryResponse();
    }
    
    @Override
    public FacetsBase getFacets() {
        return new Facets();
    }
    
    @Override
    public FieldCardinalityBase getFieldCardinality() {
        return new FieldCardinality();
    }
    
    @Override
    public KeyBase getKey() {
        return new DefaultKey();
    }
    
    @Override
    public AuthorizationsListBase getAuthorizationsList() {
        return new DefaultAuthorizationsList();
    }
    
    @Override
    public Query getQueryImpl() {
        return new QueryImpl();
    }
    
    @Override
    public DataDictionaryBase getDataDictionary() {
        return new DefaultDataDictionary();
    }
    
    @Override
    public FieldsBase getFields() {
        return new DefaultFields();
    }
    
    @Override
    public DescriptionBase getDescription() {
        return new DefaultDescription();
    }
    
    @Override
    public MetadataFieldBase getMetadataField() {
        return new DefaultMetadataField();
    }
}
