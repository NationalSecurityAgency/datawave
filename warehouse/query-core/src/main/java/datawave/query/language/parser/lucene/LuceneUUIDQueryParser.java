package datawave.query.language.parser.lucene;

import java.util.ArrayList;
import java.util.List;

import datawave.query.data.UUIDType;
import datawave.query.language.parser.ParseException;
import datawave.query.language.tree.SelectorNode;
import datawave.query.search.FieldedTerm;
import datawave.query.search.RangeFieldedTerm;
import datawave.query.search.WildcardFieldedTerm;

import org.apache.log4j.Logger;

@Deprecated
public class LuceneUUIDQueryParser extends LuceneQueryParser {
    private static Logger log = Logger.getLogger(LuceneUUIDQueryParser.class.getName());
    private List<UUIDType> uuidTypes = new ArrayList<>();

    @Override
    public datawave.query.language.tree.QueryNode parse(String query) throws ParseException {
        query = query.replaceAll("\\u0093", "\""); // replace open smart quote 147
        query = query.replaceAll("\\u0094", "\""); // replace close smart quote 148

        datawave.query.language.tree.QueryNode parsedQuery = null;
        parsedQuery = super.parse(query);
        if (!(parsedQuery instanceof SelectorNode)) {
            throw new ParseException("Query: " + query + " not supported with the LuceneQueryUUIDParser");
        }

        SelectorNode selectorNode = (SelectorNode) parsedQuery;
        FieldedTerm fieldedTerm = (FieldedTerm) selectorNode.getQuery();
        String field = fieldedTerm.getField();

        UUIDType uuidType = null;
        for (UUIDType u : uuidTypes) {
            if (u.getFieldName().equals(field)) {
                uuidType = u;
                break;
            }
        }

        if (uuidType == null) {
            throw new ParseException("Query: " + query + " not supported with the LuceneQueryUUIDParser");
        }

        if (fieldedTerm instanceof RangeFieldedTerm) {
            throw new ParseException("Query: " + query + " not supported with the LuceneQueryUUIDParser");
        }

        if (fieldedTerm instanceof WildcardFieldedTerm) {
            int firstWildcard = WildcardFieldedTerm.getFirstWildcardIndex(fieldedTerm.getSelector());
            Integer wildcardAllowedAfter = uuidType.getAllowWildcardAfter();
            if (wildcardAllowedAfter == null || firstWildcard < wildcardAllowedAfter) {
                throw new ParseException("Query: " + query + " not supported with the LuceneQueryUUIDParser");
            }
        }

        return parsedQuery;
    }

    public List<UUIDType> getUuidTypes() {
        return uuidTypes;
    }

    public void setUuidTypes(List<UUIDType> uuidTypes) {
        this.uuidTypes = uuidTypes;
    }
}
