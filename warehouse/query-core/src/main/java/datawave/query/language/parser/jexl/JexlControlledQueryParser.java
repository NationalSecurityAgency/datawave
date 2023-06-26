package datawave.query.language.parser.jexl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;
import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang.StringUtils;

public class JexlControlledQueryParser implements QueryParser, ControlledQueryParser {

    private Map<String,Set<String>> includedValues = new HashMap<>();
    private Map<String,Set<String>> excludedValues = new HashMap<>();
    private Set<String> allowedFields = new HashSet<>();

    public JexlControlledQueryParser() {
        this.allowedFields.add("_ANYFIELD_");
    }

    @Override
    public QueryNode parse(String query) throws ParseException {

        checkIfQueryAllowed(query);

        StringBuilder sb = new StringBuilder();
        if (!includedValues.isEmpty()) {
            sb.append("(");
        }
        boolean addedFirstInclude = false;
        for (Map.Entry<String,Set<String>> entry : includedValues.entrySet()) {
            String field = entry.getKey();
            for (String value : entry.getValue()) {
                if (addedFirstInclude) {
                    sb.append(" || ");
                }
                addedFirstInclude = true;
                sb.append("filter:includeRegex(").append(field).append(", '").append(value).append("')");
            }
        }
        if (!includedValues.isEmpty()) {
            sb.append(")");
        }

        if (!excludedValues.isEmpty()) {
            if (!includedValues.isEmpty()) {
                sb.append(" && ");
            }
            sb.append("(");
        }
        boolean addedFirstExclude = false;
        for (Map.Entry<String,Set<String>> entry : excludedValues.entrySet()) {
            String field = entry.getKey();
            for (String value : entry.getValue()) {
                if (addedFirstExclude) {
                    sb.append(" && ");
                }
                addedFirstExclude = true;
                sb.append("not(filter:includeRegex(").append(field).append(", '").append(value).append("'))");
            }
        }
        if (!excludedValues.isEmpty()) {
            sb.append(")");
        }

        if (sb.length() > 0) {
            query = "(" + query + ")" + " && (" + sb + ")";
        }

        QueryNode node = new ServerHeadNode();
        node.setOriginalQuery(query);
        return node;
    }

    private void checkIfQueryAllowed(String query) throws ParseException {

        JexlNode node;
        try {
            node = JexlASTHelper.parseJexlQuery(query);
        } catch (Throwable e) {
            throw new ParseException(e.getMessage());
        }

        Set<String> fields = new TreeSet<>();
        List<ASTIdentifier> idList = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier id : idList) {
            String fieldName = id.getName();
            if (!StringUtils.isEmpty(fieldName)) {
                fieldName = fieldName.trim().toUpperCase();
                if (fieldName.charAt(0) == '$') {
                    fields.add(fieldName.substring(1));
                } else {
                    fields.add(fieldName);
                }
            }
        }
        fields.removeAll(allowedFields);
        if (!fields.isEmpty()) {
            throw new ParseException("Unallowed field(s) '" + fields + "' for this type of query");
        }
    }

    @Override
    public void setExcludedValues(Map<String,Set<String>> excludedValues) {
        this.excludedValues = excludedValues;
    }

    @Override
    public Map<String,Set<String>> getExcludedValues() {
        return this.excludedValues;
    }

    @Override
    public void setIncludedValues(Map<String,Set<String>> includedValues) {
        this.includedValues = includedValues;
    }

    @Override
    public Map<String,Set<String>> getIncludedValues() {
        return this.includedValues;
    }

    @Override
    public Set<String> getAllowedFields() {
        return allowedFields;
    }

    @Override
    public void setAllowedFields(Set<String> allowedFields) {
        this.allowedFields = allowedFields;
        this.allowedFields.add("_ANYFIELD_");
    }
}
