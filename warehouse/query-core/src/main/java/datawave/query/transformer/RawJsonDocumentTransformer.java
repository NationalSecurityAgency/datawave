package datawave.query.transformer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.tables.document.batch.DocumentLogic;
import datawave.query.tables.serialization.JsonDocument;
import datawave.query.tables.serialization.RawJsonDocument;
import datawave.query.tables.serialization.SerializedDocument;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.util.StringUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.Flushable;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.logic.WritesResultCardinalities;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Transforms a document into a web service Event Object.
 *
 * Currently, this approach will support nested documents, but the nested attributes are planted in the flat structure using the name of that field from the
 * Document. Once we move toward a nested event, we can have a simpler approach.
 *
 */
public class RawJsonDocumentTransformer extends JsonDocumentTransformerSupport<SerializedDocumentIfc,String> implements WritesResultCardinalities,
                Flushable<String> {

    private static final Logger log = Logger.getLogger(DocumentTransformerSupport.class);

    boolean asDocument = true;

    /**
     * By default, assume each cell still has the visibility attached to it
     *
     * @param logic
     * @param settings
     * @param responseObjectFactory
     */
    public RawJsonDocumentTransformer(DocumentLogic logic, Query settings, MarkingFunctions markingFunctions,
                                      ResponseObjectFactory responseObjectFactory, boolean asDocument) {
        super(logic, settings, markingFunctions, responseObjectFactory);
        this.asDocument=asDocument;
    }

    public RawJsonDocumentTransformer(DocumentLogic logic, Query settings, MarkingFunctions markingFunctions,
                                      ResponseObjectFactory responseObjectFactory, Boolean reducedResponse, boolean asDocument) {
        super(logic, settings, markingFunctions, responseObjectFactory, reducedResponse);
        this.asDocument=asDocument;
    }

    public RawJsonDocumentTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                                      Boolean reducedResponse, boolean asDocument) {
        super(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
        this.asDocument=asDocument;
    }
    
    @Override
    public String flush() throws EmptyObjectException {
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
    public String transform(SerializedDocumentIfc documentEntry) throws EmptyObjectException {
        for (JsonDocumentTransform transform : transforms) {
            if (documentEntry != null) {
                documentEntry = transform.apply(documentEntry);
            } else {
                break;
            }
        }
        return _transform(documentEntry);
    }

    private String _transform(SerializedDocumentIfc documentEntry) throws EmptyObjectException {

        return documentEntry.get().toString();
    }
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        throw new UnsupportedOperationException("Not allowed");
    }

}
