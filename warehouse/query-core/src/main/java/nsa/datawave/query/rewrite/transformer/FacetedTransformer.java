package nsa.datawave.query.rewrite.transformer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nsa.datawave.data.type.StringType;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Attributes;
import nsa.datawave.query.rewrite.attributes.Cardinality;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.FieldValueCardinality;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.result.event.FacetsBase;
import nsa.datawave.webservice.query.result.event.FieldCardinalityBase;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;
import nsa.datawave.webservice.result.BaseQueryResponse;
import nsa.datawave.webservice.result.FacetQueryResponseBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class FacetedTransformer extends DocumentTransformer {
    
    private static final Logger log = Logger.getLogger(FacetedTransformer.class);
    
    /**
     * By default, assume each cell still has the visibility attached to it
     * 
     * @param logic
     * @param settings
     * @param markingFunctions
     * @param responseObjectFactory
     */
    public FacetedTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(logic, settings, markingFunctions, responseObjectFactory, false);
    }
    
    public FacetedTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean reducedResponse) {
        super(null != logic ? logic.getTableName() : null, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }
    
    public FacetedTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    Boolean reducedResponse) {
        super(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }
    
    /**
     * Accepts an attribute. The document data will be placed into the value of the Field.
     *
     * @param documentKey
     * @param document
     * @return
     */
    protected Collection<FieldCardinalityBase> buildFacets(Key documentKey, String fieldName, Document document, ColumnVisibility topLevelColumnVisibility,
                    MarkingFunctions markingFunctions) {
        
        Set<FieldCardinalityBase> myFields = new HashSet<>();
        
        final Map<String,Attribute<? extends Comparable<?>>> documentData = document.getDictionary();
        
        String fn = null;
        Attribute<?> attribute = null;
        
        for (Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
            
            attribute = data.getValue();
            myFields.addAll(buildFacets(documentKey, fn, attribute, topLevelColumnVisibility, markingFunctions));
        }
        
        return myFields;
    }
    
    /**
     * Accepts an attribute. The document data will be placed into the value of the Field.
     *
     * @param documentKey
     * @param fieldName
     * @param attr
     * @param topLevelColumnVisibility
     * @param markingFunctions
     * @return
     */
    protected Collection<FieldCardinalityBase> buildFacets(Key documentKey, String fieldName, Attribute<?> attr, ColumnVisibility topLevelColumnVisibility,
                    MarkingFunctions markingFunctions) {
        
        Set<FieldCardinalityBase> myFields = new HashSet<>();
        
        if (attr instanceof Attributes) {
            Attributes attributeList = Attributes.class.cast(attr);
            
            for (Attribute<?> embeddedAttr : attributeList.getAttributes())
                myFields.addAll(buildFacets(documentKey, fieldName, embeddedAttr, topLevelColumnVisibility, markingFunctions));
            
        } else {
            // Use the markings on the Field if we're returning the markings to the client
            if (attr instanceof Cardinality) {
                try {
                    Cardinality card = (Cardinality) attr;
                    FieldValueCardinality v = card.getContent();
                    StringType value = new StringType();
                    value.setDelegate(v.toString());
                    value.setNormalizedValue(v.toString());
                    
                    FieldCardinalityBase fc = this.responseObjectFactory.getFieldCardinality();
                    fc.setMarkings(markingFunctions.translateFromColumnVisibilityForAuths(attr.getColumnVisibility(), auths)); // reduces colvis based on
                                                                                                                               // visibility
                    fc.setColumnVisibility(new String(markingFunctions.translateToColumnVisibility(fc.getMarkings()).flatten()));
                    fc.setLower(v.getFloorValue());
                    fc.setUpper(v.getCeilingValue());
                    fc.setCardinality(v.getEstimate().cardinality());
                    
                    myFields.add(fc);
                    
                } catch (Exception e) {
                    log.error("unable to process markings:" + e);
                }
            }
            
        }
        
        return myFields;
    }
    
    protected Object buildResponse(Document document, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
                    throws MarkingFunctions.Exception {
        
        FacetsBase facetedResponse = responseObjectFactory.getFacets();
        
        final Collection<FieldCardinalityBase> documentFields = buildFacets(documentKey, null, document, eventCV, mf);
        
        facetedResponse.setMarkings(mf.translateFromColumnVisibility(eventCV));
        facetedResponse.setFields(new ArrayList<>(documentFields));
        
        // assign an estimate of the event size based on the document size
        // in practice this is about 2.5 times the size of the document estimated size
        // we need to set something here for page size trigger purposes.
        facetedResponse.setSizeInBytes(Math.round(document.sizeInBytes() * 2.5d));
        
        return facetedResponse;
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        FacetQueryResponseBase response = responseObjectFactory.getFacetQueryResponse();
        Set<ColumnVisibility> combinedColumnVisibility = new HashSet<ColumnVisibility>();
        
        for (Object result : resultList) {
            FacetsBase facet = (FacetsBase) result;
            response.addFacet(facet);
            
            // Turns out that the column visibility is not set on the key returned & not added to markings map
            // so loop through the fields instead
            
            for (FieldCardinalityBase fcb : facet.getFields()) {
                if (StringUtils.isNotBlank(fcb.getColumnVisibility())) {
                    combinedColumnVisibility.add(new ColumnVisibility(fcb.getColumnVisibility()));
                }
            }
        }
        
        try {
            ColumnVisibility columnVisibility = this.markingFunctions.combine(combinedColumnVisibility);
            response.setMarkings(this.markingFunctions.translateFromColumnVisibility(columnVisibility));
            
        } catch (MarkingFunctions.Exception e) {
            log.warn(e);
            // original ignored these exceptions
        }
        
        return response;
    }
    
}
