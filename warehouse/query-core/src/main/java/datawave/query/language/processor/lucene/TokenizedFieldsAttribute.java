package datawave.query.language.processor.lucene;

import java.util.Collection;

import org.apache.lucene.util.Attribute;

public interface TokenizedFieldsAttribute extends Attribute {
    public void setTokenizedFields(Collection<String> fields);
    
    public Collection<String> getTokenizedFields();
    
    public void setTokenizeUnfieldedQueriesEnabled(boolean tokenizeUnfieldedQueriesEnabled);
    
    public boolean isTokenizeUnfieldedQueriesEnabled();
    
    public void setSkipTokenizeUnfieldedFields(Collection<String> fields);
    
    public Collection<String> getSkipTokenizeUnfieldedFields();
}
