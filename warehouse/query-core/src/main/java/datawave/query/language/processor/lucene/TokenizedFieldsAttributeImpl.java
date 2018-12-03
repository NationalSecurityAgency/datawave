package datawave.query.language.processor.lucene;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.AttributeImpl;

public class TokenizedFieldsAttributeImpl extends AttributeImpl implements TokenizedFieldsAttribute {
    
    private static final long serialVersionUID = 787193567135704985L;
    
    private final Set<String> tokenizedFields = new HashSet<>();
    private boolean tokenizeUnfieldedQueriesEnabled = false;
    private final Set<String> skipTokenizeUnfieldedFields = new HashSet<>();
    
    @Override
    public void setTokenizedFields(Collection<String> fields) {
        tokenizedFields.clear();
        tokenizedFields.addAll(fields);
    }
    
    @Override
    public Collection<String> getTokenizedFields() {
        return tokenizedFields;
    }
    
    @Override
    public void setSkipTokenizeUnfieldedFields(Collection<String> fields) {
        skipTokenizeUnfieldedFields.clear();
        skipTokenizeUnfieldedFields.addAll(fields);
    }
    
    @Override
    public Collection<String> getSkipTokenizeUnfieldedFields() {
        return skipTokenizeUnfieldedFields;
    }
    
    @Override
    public void setTokenizeUnfieldedQueriesEnabled(boolean tokenizeUnfieldedQueriesEnabled) {
        this.tokenizeUnfieldedQueriesEnabled = tokenizeUnfieldedQueriesEnabled;
    }
    
    @Override
    public boolean isTokenizeUnfieldedQueriesEnabled() {
        return tokenizeUnfieldedQueriesEnabled;
    }
    
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void copyTo(AttributeImpl target) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String toString() {
        return "<tokenizedFieldsAttribute tokenizedFields='" + tokenizedFields + "' tokenizeUnfieldedQueryes='" + tokenizeUnfieldedQueriesEnabled + "'/>";
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (tokenizeUnfieldedQueriesEnabled ? 1231 : 1237);
        result = prime * result + (tokenizedFields.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TokenizedFieldsAttributeImpl other = (TokenizedFieldsAttributeImpl) obj;
        if (tokenizeUnfieldedQueriesEnabled != other.tokenizeUnfieldedQueriesEnabled)
            return false;
        if (!tokenizedFields.equals(other.tokenizedFields))
            return false;
        return true;
    }
}
