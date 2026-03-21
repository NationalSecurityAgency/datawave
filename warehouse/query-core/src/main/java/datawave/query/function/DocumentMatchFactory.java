package datawave.query.function;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.util.Tuple3;

/**
 * Builds the pre-evaluation function that populates {@link DocumentMatchContext} for {@code document:match(...)} evaluation.
 */
public class DocumentMatchFactory {
    private DocumentMatchFactory() {}

    /**
     * Returns a context-populating function for document matching.
     *
     * @param config
     *            document-match configuration
     * @return either a context-populating function or a no-op function when no source is available
     */
    public static Function<Tuple3<Key,Document,Map<String,Object>>,Tuple3<Key,Document,Map<String,Object>>> getFunction(DocumentMatchConfig config) {
        if (config == null || config.getSource() == null) {
            return new EmptyDocumentMatchFunction();
        }
        return new DocumentMatchContextFunction(config);
    }
}
