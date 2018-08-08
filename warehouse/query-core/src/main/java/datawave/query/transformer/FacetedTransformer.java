package datawave.query.transformer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Preconditions;
import datawave.data.type.StringType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Cardinality;
import datawave.query.attributes.Content;
import datawave.query.attributes.FieldValueCardinality;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.model.QueryModel;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.FacetsBase;
import datawave.webservice.query.result.event.FieldCardinalityBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.FacetQueryResponseBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class FacetedTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,FacetsBase> {
    
    private static final Logger log = Logger.getLogger(FacetedTransformer.class);
    
    private DocumentTransformer documentTransformer;
    
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
        super(markingFunctions);
        this.documentTransformer = new DocumentTransformer(logic, settings, markingFunctions, responseObjectFactory, false);
    }
    
    public FacetedTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean reducedResponse) {
        super(markingFunctions);
        this.documentTransformer = new DocumentTransformer(null != logic ? logic.getTableName() : null, settings, markingFunctions, responseObjectFactory,
                        reducedResponse);
    }
    
    public FacetedTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    Boolean reducedResponse) {
        super(markingFunctions);
        this.documentTransformer = new DocumentTransformer(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
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
                    
                    FieldCardinalityBase fc = documentTransformer.getResponseObjectFactory().getFieldCardinality();
                    fc.setMarkings(markingFunctions.translateFromColumnVisibilityForAuths(attr.getColumnVisibility(), documentTransformer.getAuths())); // reduces
                    // colvis
                    // based on
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
    
    protected FacetsBase buildResponse(Document document, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
                    throws MarkingFunctions.Exception {
        
        FacetsBase facetedResponse = documentTransformer.getResponseObjectFactory().getFacets();
        
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
        FacetQueryResponseBase response = documentTransformer.getResponseObjectFactory().getFacetQueryResponse();
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
    
    @Override
    public FacetsBase transform(Entry<Key,Value> input) {
        if (null == input)
            throw new IllegalArgumentException("Input cannot be null");
        
        FacetsBase output = null;
        
        Key documentKey = null;
        Document document = null;
        String dataType = null;
        String uid = null;
        
        @SuppressWarnings("unchecked")
        Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
        
        Entry<Key,Document> documentEntry = documentTransformer.getDocumentDeserializer().apply(entry);
        
        documentKey = documentTransformer.correctKey(documentEntry.getKey());
        document = documentEntry.getValue();
        
        if (null == documentKey || null == document)
            throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + entry.getValue());
        
        documentTransformer.extractMetrics(document, documentKey);
        document.debugDocumentSize(documentKey);
        
        String row = documentKey.getRow().toString();
        
        String colf = documentKey.getColumnFamily().toString();
        
        int index = colf.indexOf("\0");
        Preconditions.checkArgument(-1 != index);
        
        dataType = colf.substring(0, index);
        uid = colf.substring(index + 1);
        
        // We don't have to consult the Document to rebuild the Visibility, the key
        // should have the correct top-level visibility
        ColumnVisibility eventCV = new ColumnVisibility(documentKey.getColumnVisibility());
        
        try {
            
            for (String contentFieldName : documentTransformer.getContentFieldNames()) {
                if (document.containsKey(contentFieldName)) {
                    Attribute<?> contentField = document.remove(contentFieldName);
                    if (contentField.getData().toString().equalsIgnoreCase("true")) {
                        Content c = new Content(uid, contentField.getMetadata(), document.isToKeep());
                        document.put(contentFieldName, c, false, documentTransformer.isReducedResponse());
                    }
                }
            }
            
            for (String primaryField : documentTransformer.getPrimaryToSecondaryFieldMap().keySet()) {
                if (!document.containsKey(primaryField)) {
                    for (String secondaryField : documentTransformer.getPrimaryToSecondaryFieldMap().get(primaryField)) {
                        if (document.containsKey(secondaryField)) {
                            document.put(primaryField, document.get(secondaryField), false, documentTransformer.isReducedResponse());
                            break;
                        }
                    }
                }
            }
            
            // build response method here
            output = buildResponse(document, documentKey, eventCV, colf, row, this.markingFunctions);
            
        } catch (Exception ex) {
            log.error("Error building response document", ex);
            throw new RuntimeException(ex);
        }
        
        if (output == null) {
            // buildResponse will return a null object if there was only metadata in the document
            throw new EmptyObjectException();
        }
        
        if (documentTransformer.getCardinalityConfiguration() != null) {
            documentTransformer.collectCardinalities(document, documentKey, uid, dataType);
        }
        
        return output;
    }
    
    // @Override
    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.documentTransformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        
    }
    
    public QueryModel getQm() {
        return this.documentTransformer.getQm();
    }
    
    public void setQm(QueryModel qm) {
        this.documentTransformer.setQm(qm);
    }
    
}
