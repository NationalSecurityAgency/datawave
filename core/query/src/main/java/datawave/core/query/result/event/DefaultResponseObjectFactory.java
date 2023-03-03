package datawave.core.query.result.event;

import datawave.core.query.cachedresults.CacheableQueryRowImpl;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DefaultFields;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.DefaultMetadataField;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.DefaultEdge;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultFacets;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.DefaultFieldCardinality;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FacetsBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.FieldCardinalityBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.KeyBase;
import datawave.webservice.result.DefaultEdgeQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.FacetQueryResponse;
import datawave.webservice.result.FacetQueryResponseBase;

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
        return new DefaultFacets();
    }
    
    @Override
    public FieldCardinalityBase getFieldCardinality() {
        return new DefaultFieldCardinality();
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
