package datawave.core.query.language.processor.lucene;

import java.util.Collection;

import org.apache.lucene.util.Attribute;

public interface TokenizedFieldsAttribute extends Attribute {
    void setTokenizedFields(Collection<String> fields);

    Collection<String> getTokenizedFields();

    void setTokenizeUnfieldedQueriesEnabled(boolean tokenizeUnfieldedQueriesEnabled);

    boolean isTokenizeUnfieldedQueriesEnabled();

    void setSkipTokenizeUnfieldedFields(Collection<String> fields);

    Collection<String> getSkipTokenizeUnfieldedFields();
}
