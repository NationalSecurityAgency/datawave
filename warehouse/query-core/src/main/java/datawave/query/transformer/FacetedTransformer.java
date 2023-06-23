package datawave.query.transformer;

import com.google.common.base.Preconditions;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.data.type.StringType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Cardinality;
import datawave.query.attributes.Document;
import datawave.query.attributes.FieldValueCardinality;
import datawave.webservice.query.Query;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class FacetedTransformer extends DocumentTransformerSupport<Entry<Key,Value>,FacetsBase> {

    private static final Logger log = Logger.getLogger(FacetedTransformer.class);

    /*
     * By default, assume each cell still has the visibility attached to it
     */
    public FacetedTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(logic, settings, markingFunctions, responseObjectFactory);
    }

    public FacetedTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean reducedResponse) {
        super(logic, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }

    public FacetedTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    Boolean reducedResponse) {
        super(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }

    /**
     * Accepts an attribute. The document data will be placed into the value of the Field.
     *
     * @param documentKey
     *            the document key
     * @param fieldName
     *            a field name
     * @param markingFunctions
     *            marking functions
     * @param document
     *            a document
     * @param topLevelColumnVisibility
     *            top level column vis
     * @return list of facets
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
     *            the document key
     * @param fieldName
     *            a field name
     * @param attr
     *            an attribute
     * @param topLevelColumnVisibility
     *            the top level visibility
     * @param markingFunctions
     *            marking functions
     * @return list of facets
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
                    fc.setField(v.getFieldName());
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

    protected FacetsBase buildResponse(Document document, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
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
        Set<ColumnVisibility> combinedColumnVisibility = new HashSet<>();

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
    public FacetsBase transform(Entry<Key,Value> entry) throws EmptyObjectException {

        Entry<Key,Document> documentEntry = deserializer.apply(entry);
        for (DocumentTransform transform : transforms) {
            if (documentEntry != null) {
                documentEntry = transform.apply(documentEntry);
            } else {
                break;
            }
        }

        return _transform(documentEntry);
    }

    private FacetsBase _transform(Entry<Key,Document> documentEntry) throws EmptyObjectException {
        if (documentEntry == null) {
            // buildResponse will return a null object if there was only metadata in the document
            throw new EmptyObjectException();
        }

        Key documentKey = correctKey(documentEntry.getKey());
        Document document = documentEntry.getValue();

        if (null == documentKey || null == document)
            throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + documentEntry.getValue());

        extractMetrics(document, documentKey);
        document.debugDocumentSize(documentKey);

        String row = documentKey.getRow().toString();

        String colf = documentKey.getColumnFamily().toString();

        int index = colf.indexOf("\0");
        Preconditions.checkArgument(-1 != index);

        String dataType = colf.substring(0, index);
        String uid = colf.substring(index + 1);

        // We don't have to consult the Document to rebuild the Visibility, the key
        // should have the correct top-level visibility
        ColumnVisibility eventCV = new ColumnVisibility(documentKey.getColumnVisibility());

        FacetsBase output = null;
        try {
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

        if (cardinalityConfiguration != null) {
            collectCardinalities(document, documentKey, uid, dataType);
        }

        return output;
    }

    // @Override
    // public FacetsBase transform2(Entry<Key,Value> input) {
    // if (null == input)
    // throw new IllegalArgumentException("Input cannot be null");
    //
    // FacetsBase output = null;
    //
    // Key documentKey = null;
    // Document document = null;
    // String dataType = null;
    // String uid = null;
    //
    // @SuppressWarnings("unchecked")
    // Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
    //
    // Entry<Key,Document> documentEntry = deserializer.apply(entry);
    //
    // documentKey = correctKey(documentEntry.getKey());
    // document = documentEntry.getValue();
    //
    // if (null == documentKey || null == document)
    // throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + entry.getValue());
    //
    // extractMetrics(document, documentKey);
    // document.debugDocumentSize(documentKey);
    //
    // String row = documentKey.getRow().toString();
    //
    // String colf = documentKey.getColumnFamily().toString();
    //
    // int index = colf.indexOf("\0");
    // Preconditions.checkArgument(-1 != index);
    //
    // dataType = colf.substring(0, index);
    // uid = colf.substring(index + 1);
    //
    // // We don't have to consult the Document to rebuild the Visibility, the key
    // // should have the correct top-level visibility
    // ColumnVisibility eventCV = new ColumnVisibility(documentKey.getColumnVisibility());
    //
    // try {
    //
    // for (String contentFieldName : this.contentFieldNames) {
    // if (document.containsKey(contentFieldName)) {
    // Attribute<?> contentField = document.remove(contentFieldName);
    // if (contentField.getData().toString().equalsIgnoreCase("true")) {
    // Content c = new Content(uid, contentField.getMetadata(), document.isToKeep());
    // document.put(contentFieldName, c, false, this.reducedResponse);
    // }
    // }
    // }
    //
    // for (String primaryField : this.primaryToSecondaryFieldMap.keySet()) {
    // if (!document.containsKey(primaryField)) {
    // for (String secondaryField : this.primaryToSecondaryFieldMap.get(primaryField)) {
    // if (document.containsKey(secondaryField)) {
    // document.put(primaryField, document.get(secondaryField), false, this.reducedResponse);
    // break;
    // }
    // }
    // }
    // }
    //
    // // build response method here
    // output = buildResponse(document, documentKey, eventCV, colf, row, this.markingFunctions);
    //
    // } catch (Exception ex) {
    // log.error("Error building response document", ex);
    // throw new RuntimeException(ex);
    // }
    //
    // if (output == null) {
    // // buildResponse will return a null object if there was only metadata in the document
    // throw new EmptyObjectException();
    // }
    //
    // if (cardinalityConfiguration != null) {
    // collectCardinalities(document, documentKey, uid, dataType);
    // }
    //
    // return output;
    // }
}
