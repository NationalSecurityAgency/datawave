package datawave.query.language.processor.lucene;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PhraseSlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;

/**
 *
 */
public class CustomFieldLimiterNodeProcessor extends QueryNodeProcessorImpl {
    
    private Set<String> allowedFields = null;
    private Boolean allowAnyFieldQueries = true;
    
    @Override
    protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
        
        if (getQueryConfigHandler().has(ConfigurationKeys.ENABLE_POSITION_INCREMENTS)) {
            
            if (getQueryConfigHandler().has(LuceneToJexlQueryParser.ALLOWED_FIELDS)) {
                allowedFields = new HashSet<>();
                allowedFields.addAll(getQueryConfigHandler().get(LuceneToJexlQueryParser.ALLOWED_FIELDS));
            }
            if (getQueryConfigHandler().has(LuceneToJexlQueryParser.ALLOW_ANY_FIELD_QUERIES)) {
                allowAnyFieldQueries = getQueryConfigHandler().get(LuceneToJexlQueryParser.ALLOW_ANY_FIELD_QUERIES);
            }
        }
        return node;
    }
    
    @Override
    protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
        
        List<String> fields = new ArrayList<>();
        
        if (node instanceof FieldQueryNode) {
            fields.add(((FieldQueryNode) node).getFieldAsString());
        } else if (node instanceof FunctionQueryNode) {
            List<String> parameterList = ((FunctionQueryNode) node).getParameterList();
            String function = ((FunctionQueryNode) node).getFunction();
            if (function.equalsIgnoreCase("include") || function.equalsIgnoreCase("exclude")) {
                if (parameterList.size() % 2 == 1) {
                    for (int x = 1; x < parameterList.size(); x++) {
                        fields.add(parameterList.get(x));
                    }
                } else {
                    fields.add(parameterList.get(0));
                }
            } else if (function.equalsIgnoreCase("isnull") || function.equalsIgnoreCase("isnotnull")) {
                fields.add(parameterList.get(0));
            }
        } else if (node instanceof MultiPhraseQueryNode) {
            fields.add(((MultiPhraseQueryNode) node).getField().toString());
        } else if (node instanceof PointQueryNode) {
            fields.add(((PointQueryNode) node).getField().toString());
        } else if (node instanceof PhraseSlopQueryNode) {
            fields.add(((PhraseSlopQueryNode) node).getField().toString());
        } else if (node instanceof RegexpQueryNode) {
            fields.add(((RegexpQueryNode) node).getField().toString());
        } else if (node instanceof SlopQueryNode) {
            fields.add(((SlopQueryNode) node).getField().toString());
        } else if (node instanceof TokenizedPhraseQueryNode) {
            fields.add(((TokenizedPhraseQueryNode) node).getField().toString());
        }
        
        for (String f : fields) {
            if (f.isEmpty()) {
                if (!allowAnyFieldQueries) {
                    throw new IllegalArgumentException("Unfielded terms are not permitted in this type of query");
                }
            } else if (allowedFields != null && !allowedFields.contains(f.toUpperCase())) {
                throw new IllegalArgumentException("Field '" + f + "' is not permitted in this type of query");
            }
        }
        
        return node;
    }
    
    @Override
    protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {
        return children;
    }
}
