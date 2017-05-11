package datawave.query.language.parser.lucene;

import org.apache.lucene.queryparser.flexible.standard.config.FieldBoostMapFCListener;
import org.apache.lucene.queryparser.flexible.standard.config.FieldDateResolutionFCListener;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;

public class QueryConfigHandler extends StandardQueryConfigHandler {
    public QueryConfigHandler() {
        // Add listener that will build the FieldConfig attributes.
        addFieldConfigListener(new FieldBoostMapFCListener(this));
        addFieldConfigListener(new FieldDateResolutionFCListener(this));
        
        set(ConfigurationKeys.DEFAULT_OPERATOR, Operator.AND);
        set(ConfigurationKeys.LOWERCASE_EXPANDED_TERMS, Boolean.TRUE);
    }
}
