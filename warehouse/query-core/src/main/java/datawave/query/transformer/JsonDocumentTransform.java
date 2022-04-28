package datawave.query.transformer;

import com.google.common.base.Function;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Map;

public interface JsonDocumentTransform extends Function<SerializedDocumentIfc,SerializedDocumentIfc> {
    // called when adding the document transform
    void initialize(Query settings, MarkingFunctions markingFunctions);
    
    // called after the last document is passed through to get any remaining aggregated results.
    SerializedDocumentIfc flush();
    
    class DefaultDocumentTransform implements JsonDocumentTransform {
        protected Query settings;
        protected MarkingFunctions markingFunctions;
        
        @Override
        public void initialize(Query settings, MarkingFunctions markingFunctions) {
            this.settings = settings;
            this.markingFunctions = markingFunctions;
        }
        
        @Override
        public SerializedDocumentIfc flush() {
            return null;
        }
        
        @Nullable
        @Override
        public SerializedDocumentIfc apply(@Nullable SerializedDocumentIfc keyDocumentEntry) {
            return keyDocumentEntry;
        }
    }
    
}
