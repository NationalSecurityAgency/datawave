package datawave.query.function;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;

/**
 *
 *
 */
public class NoOpMaskedValueFilter implements MaskedValueFilterInterface {

    private boolean includeGroupingContext;
    private boolean reducedResponse;

    public NoOpMaskedValueFilter() {
        this(false, false);
    }

    public NoOpMaskedValueFilter(boolean _includeGroupingContext, boolean _reducedResponse) {
        this.includeGroupingContext = _includeGroupingContext;
        this.reducedResponse = _reducedResponse;
    }

    @Override
    public void setIncludeGroupingContext(boolean includeGroupingContext) {
        this.includeGroupingContext = includeGroupingContext;
    }

    @Override
    public boolean isIncludeGroupingContext() {
        return includeGroupingContext;
    }

    @Override
    public void setReducedResponse(boolean reducedResponse) {
        this.reducedResponse = reducedResponse;
    }

    @Override
    public boolean isReducedResponse() {
        return reducedResponse;
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(Map.Entry<Key,Document> keyDocumentEntry) {
        return keyDocumentEntry;
    }
}
