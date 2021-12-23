package datawave.query.transformer;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.tables.document.batch.DocumentLogic;
import datawave.query.tables.serialization.JsonDocument;
import datawave.query.tables.serialization.SerializedDocument;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.util.StringUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.Flushable;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.logic.WritesResultCardinalities;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Transforms a document into a web service Event Object.
 *
 * Currently, this approach will support nested documents, but the nested attributes are planted in the flat structure using the name of that field from the
 * Document. Once we move toward a nested event, we can have a simpler approach.
 *
 */
public class JsonDocumentTransformer extends JsonDocumentTransformerSupport<SerializedDocumentIfc,EventBase> implements WritesQueryMetrics, WritesResultCardinalities,
                Flushable<EventBase> {

    private static final Logger log = Logger.getLogger(DocumentTransformerSupport.class);

    boolean asDocument = true;

    /**
     * By default, assume each cell still has the visibility attached to it
     *
     * @param logic
     * @param settings
     * @param responseObjectFactory
     */
    public JsonDocumentTransformer(DocumentLogic logic, Query settings, MarkingFunctions markingFunctions,
                                   ResponseObjectFactory responseObjectFactory, boolean asDocument) {
        super(logic, settings, markingFunctions, responseObjectFactory);
        this.asDocument=asDocument;
    }

    public JsonDocumentTransformer(DocumentLogic logic, Query settings, MarkingFunctions markingFunctions,
                                   ResponseObjectFactory responseObjectFactory, Boolean reducedResponse, boolean asDocument) {
        super(logic, settings, markingFunctions, responseObjectFactory, reducedResponse);
        this.asDocument=asDocument;
    }

    public JsonDocumentTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                                   Boolean reducedResponse, boolean asDocument) {
        super(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
        this.asDocument=asDocument;
    }
    
    @Override
    public EventBase flush() throws EmptyObjectException {
        SerializedDocumentIfc documentEntry = null;
        boolean flushedObjectFound = false;
        for (JsonDocumentTransform transform : transforms) {
            if (documentEntry == null) {
                // if we had found a flushed object, but a subsequent transform returned null, then we need to try this again later.
                // throwing EmptyObjectException will force the DatawaveTransformIterator to do as such
                if (flushedObjectFound) {
                    throw new EmptyObjectException();
                }
                documentEntry = transform.flush();
                if (documentEntry != null) {
                    flushedObjectFound = true;
                }
            } else {
                documentEntry = transform.apply(documentEntry);
            }
        }
        
        if (flushedObjectFound) {
            return _transform(documentEntry);
        } else {
            return null;
        }
    }
    
    @Override
    public EventBase transform(SerializedDocumentIfc documentEntry) throws EmptyObjectException {
        for (JsonDocumentTransform transform : transforms) {
            if (documentEntry != null) {
                documentEntry = transform.apply(documentEntry);
            } else {
                break;
            }
        }
        return _transform(documentEntry);
    }

    private EventBase _transform(SerializedDocumentIfc documentEntry) throws EmptyObjectException {
            if (asDocument || (!asDocument && documentEntry instanceof SerializedDocument)){
                return _transform( documentEntry.computeKey(), documentEntry.getAsDocument());
            }else{
                return _transform(documentEntry.computeKey(),documentEntry, documentEntry.get());
            }
    }

    private EventBase _transform(Key key, Document documentEntry) throws EmptyObjectException {
        if (documentEntry == null) {
            // buildResponse will return a null object if there was only metadata in the document
            throw new EmptyObjectException();
        }
        
        Key documentKey = correctKey(key);
        Document document = documentEntry;
        
        if (null == documentKey || null == document)
            throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + documentEntry);
        
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
        
        EventBase output = null;
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

    private EventBase _transform(Key key, SerializedDocumentIfc original, JsonObject documentEntry) throws EmptyObjectException {
        if (documentEntry == null) {
            // buildResponse will return a null object if there was only metadata in the document
            throw new EmptyObjectException();
        }

        Key documentKey = correctKey(key);
        JsonObject document = documentEntry;

        if (null == documentKey || null == document)
            throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + documentEntry);

        String row = documentKey.getRow().toString();

        String colf = documentKey.getColumnFamily().toString();

        int index = colf.indexOf("\0");
        Preconditions.checkArgument(-1 != index);

        String dataType = colf.substring(0, index);
        String uid = colf.substring(index + 1);

        // We don't have to consult the Document to rebuild the Visibility, the key
        // should have the correct top-level visibility
        ColumnVisibility eventCV = new ColumnVisibility(documentKey.getColumnVisibility());

        EventBase output = null;
        try {
            // build response method here
            output = buildResponse(document,original, documentKey, eventCV, colf, row, this.markingFunctions);
        } catch (Exception ex) {
            log.error("Error building response document", ex);
            throw new RuntimeException(ex);
        }

        if (output == null) {
            // buildResponse will return a null object if there was only metadata in the document
            throw new EmptyObjectException();
        }


        return output;
    }


    protected EventBase buildResponse(Document document, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
                    throws MarkingFunctions.Exception {
        
        Map<String,String> markings = mf.translateFromColumnVisibility(eventCV);
        
        EventBase event = null;
        final Collection<FieldBase<?>> documentFields = buildDocumentFields(documentKey, null, document, eventCV, mf);
        // if documentFields is empty, then the response contained only timing metadata
        if (!documentFields.isEmpty()) {
            event = this.responseObjectFactory.getEvent();
            event.setMarkings(markings);
            event.setFields(new ArrayList<>(documentFields));
            
            Metadata metadata = new Metadata();
            String[] colfParts = StringUtils.split(colf, '\0');
            if (colfParts.length >= 1) {
                metadata.setDataType(colfParts[0]);
            }
            
            if (colfParts.length >= 2) {
                metadata.setInternalId(colfParts[1]);
            }
            
            if (this.tableName != null) {
                metadata.setTable(this.tableName);
            }
            metadata.setRow(row);
            event.setMetadata(metadata);
            
            if (eventQueryDataDecoratorTransformer != null) {
                event = (EventBase) eventQueryDataDecoratorTransformer.transform(event);
            }
            
            // assign an estimate of the event size based on the document size
            // in practice this is about 2.5 times the size of the document estimated size
            // we need to set something here for page size trigger purposes.
            event.setSizeInBytes(Math.round(document.sizeInBytes() * 2.5d));
        }
        
        return event;
    }

    protected EventBase buildResponse(JsonObject document,SerializedDocumentIfc original, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
            throws MarkingFunctions.Exception {

        Map<String,String> markings = mf.translateFromColumnVisibility(eventCV);

        EventBase event = null;
        final Collection<FieldBase<?>> documentFields = buildDocumentFields(documentKey, null, document, eventCV, mf);
        // if documentFields is empty, then the response contained only timing metadata
        if (!documentFields.isEmpty()) {
            event = this.responseObjectFactory.getEvent();
            event.setMarkings(markings);
            event.setFields(new ArrayList<>(documentFields));

            Metadata metadata = new Metadata();
            String[] colfParts = StringUtils.split(colf, '\0');
            if (colfParts.length >= 1) {
                metadata.setDataType(colfParts[0]);
            }

            if (colfParts.length >= 2) {
                metadata.setInternalId(colfParts[1]);
            }

            if (this.tableName != null) {
                metadata.setTable(this.tableName);
            }
            metadata.setRow(row);
            event.setMetadata(metadata);

            if (eventQueryDataDecoratorTransformer != null) {
                event = (EventBase) eventQueryDataDecoratorTransformer.transform(event);
            }

            // assign an estimate of the event size based on the document size
            // in practice this is about 2.5 times the size of the document estimated size
            // we need to set something here for page size trigger purposes.
            event.setSizeInBytes(Math.round(original.size() * 2.5d));
        }

        return event;
    }
    
}
