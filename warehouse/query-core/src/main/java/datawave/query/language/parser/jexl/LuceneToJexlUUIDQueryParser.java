package datawave.query.language.parser.jexl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.query.Constants;
import datawave.query.data.UUIDType;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.lucene.LuceneQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.SelectorNode;
import datawave.query.search.FieldedTerm;
import datawave.query.search.RangeFieldedTerm;
import datawave.query.search.WildcardFieldedTerm;

public class LuceneToJexlUUIDQueryParser extends LuceneToJexlQueryParser {
    private Map<String,UUIDType> uuidTypes = new HashMap<>();
    private LuceneQueryParser luceneParser = new LuceneQueryParser();

    @Override
    public QueryNode parse(String query) throws ParseException {
        query = query.replaceAll(Constants.UTF_16_SMART_QUOTE_LEFT, Constants.QUOTE); // replace open smart quote 147
        query = query.replaceAll(Constants.UTF_16_SMART_QUOTE_RIGHT, Constants.QUOTE); // replace close smart quote 148

        QueryNode parsedQuery = null;

        parsedQuery = luceneParser.parse(query);
        if (!validUUIDQuery(parsedQuery))
            throw new ParseException("Query: " + query + " not supported with the LuceneToJexlUUIDQueryParser");

        return super.parse(query);
    }

    public Map<String,UUIDType> getUuidTypes() {
        return uuidTypes;
    }

    public void setUuidTypes(Map<String,UUIDType> uuidTypes) {
        this.uuidTypes = uuidTypes;
    }

    private boolean validUUIDSelectorNode(QueryNode node) {
        SelectorNode selectorNode = (SelectorNode) node;
        FieldedTerm fieldedTerm = (FieldedTerm) selectorNode.getQuery();
        String field = fieldedTerm.getField();

        UUIDType uuidType = uuidTypes.get(field.toUpperCase());

        if (uuidType == null) {
            return false;
        }

        if (fieldedTerm instanceof RangeFieldedTerm) {
            return false;
        }

        if (fieldedTerm instanceof WildcardFieldedTerm) {
            int firstWildcard = WildcardFieldedTerm.getFirstWildcardIndex(fieldedTerm.getSelector());
            Integer wildcardAllowedAfter = uuidType.getAllowWildcardAfter();
            if (wildcardAllowedAfter == null || firstWildcard < wildcardAllowedAfter) {
                return false;
            }
        }
        return true;
    }

    private boolean validUUIDQuery(QueryNode node) {
        if (node != null) {
            if (node.isLeaf()) {
                if (node instanceof SelectorNode) {
                    return validUUIDSelectorNode(node);
                }
                // is leaf but not a SelectorNode
                else {
                    return false;
                }
            }
            // recursively validate children as UUID selector nodes
            else {
                List<QueryNode> children = node.getChildren();
                for (QueryNode child : children) {
                    if (!validUUIDQuery(child)) {
                        return false;
                    }
                }
                return true;
            }
        }
        // isn't a QueryNode
        else {
            return false;
        }
    }
}
