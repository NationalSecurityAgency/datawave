package datawave.query.transformer;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.marking.MarkingFunctions;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.function.LogTiming;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.cardinality.CardinalityConfiguration;
import datawave.query.cardinality.CardinalityRecord;
import datawave.query.jexl.JexlASTHelper;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.logic.WritesResultCardinalities;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Transforms a document into a web service Event Object.
 *
 * Currently, this approach will support nested documents, but the nested attributes are planted in the flat structure using the name of that field from the
 * Document. Once we move toward a nested event, we can have a simpler approach.
 *
 */
public class DocumentTransformer extends DocumentTransformerSupport<Entry<Key,Value>,EventBase> implements WritesQueryMetrics, WritesResultCardinalities {
    
    private static final Logger log = Logger.getLogger(DocumentTransformerSupport.class);
    
    /**
     * By default, assume each cell still has the visibility attached to it
     *
     * @param logic
     * @param settings
     * @param responseObjectFactory
     */
    public DocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(logic, settings, markingFunctions, responseObjectFactory);
    }
    
    public DocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean reducedResponse) {
        super(logic, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }
    
    public DocumentTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    Boolean reducedResponse) {
        super(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }
    
    @Override
    public EventBase transform(Entry<Key,Value> input) {
        if (null == input)
            throw new IllegalArgumentException("Input cannot be null");
        
        EventBase output = null;
        
        Key documentKey = null;
        Document document = null;
        String dataType = null;
        String uid = null;
        
        @SuppressWarnings("unchecked")
        Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
        
        Entry<Key,Document> documentEntry = deserializer.apply(entry);
        
        documentKey = correctKey(documentEntry.getKey());
        document = documentEntry.getValue();
        
        if (null == documentKey || null == document)
            throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + entry.getValue());
        
        extractMetrics(document, documentKey);
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
            
            for (String contentFieldName : this.contentFieldNames) {
                if (document.containsKey(contentFieldName)) {
                    Attribute<?> contentField = document.remove(contentFieldName);
                    if (contentField.getData().toString().equalsIgnoreCase("true")) {
                        Content c = new Content(uid, contentField.getMetadata(), document.isToKeep());
                        document.put(contentFieldName, c, false, this.reducedResponse);
                    }
                }
            }
            
            for (String primaryField : this.primaryToSecondaryFieldMap.keySet()) {
                if (!document.containsKey(primaryField)) {
                    for (String secondaryField : this.primaryToSecondaryFieldMap.get(primaryField)) {
                        if (document.containsKey(secondaryField)) {
                            document.put(primaryField, document.get(secondaryField), false, this.reducedResponse);
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
        
        if (cardinalityConfiguration != null) {
            collectCardinalities(document, documentKey, uid, dataType);
        }
        
        return output;
    }
    
    protected EventBase buildResponse(Document document, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
                    throws MarkingFunctions.Exception {
        
        Map<String,String> markings = mf.translateFromColumnVisibility(eventCV);
        
        EventBase event = null;
        final Collection<FieldBase<?>> documentFields = buildDocumentFields(documentKey, null, document, eventCV, mf);
        // if documentFields is empty, then the response contained only timing metadata
        if (documentFields.size() > 0) {
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
    
}
