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
        // This config no longer exists and is apparently (?) no longer needed....
        // See 28/Jun/2016 comment from Adrien Grand on https://jira.apache.org/jira/browse/LUCENE-7355
        // set(ConfigurationKeys.LOWERCASE_EXPANDED_TERMS, Boolean.TRUE);
    }
}
