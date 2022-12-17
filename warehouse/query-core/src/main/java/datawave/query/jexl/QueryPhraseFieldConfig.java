package datawave.query.jexl;

import java.util.HashMap;
import java.util.Map;

public class QueryPhraseFieldConfig {
    
    protected final Map<String,String> phraseMappings;
    
    public QueryPhraseFieldConfig() {
        phraseMappings = new HashMap<>();
    }
    
    public String getFieldPhrase(String fieldName) {
        return fieldName;
    }
}
