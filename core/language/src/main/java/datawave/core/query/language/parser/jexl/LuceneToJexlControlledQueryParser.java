package datawave.core.query.language.parser.jexl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import datawave.core.query.language.parser.ParseException;
import datawave.core.query.language.tree.QueryNode;

public class LuceneToJexlControlledQueryParser extends LuceneToJexlQueryParser implements ControlledQueryParser {

    private Map<String,Set<String>> includedValues = new HashMap<>();
    private Map<String,Set<String>> excludedValues = new HashMap<>();

    public LuceneToJexlControlledQueryParser() {
        super();
        this.setAllowAnyField(true);
    }

    @Override
    public QueryNode parse(String query) throws ParseException {

        StringBuilder sb = new StringBuilder();
        boolean addedFirstInclude = false;
        for (Map.Entry<String,Set<String>> entry : includedValues.entrySet()) {
            String field = entry.getKey();
            for (String value : entry.getValue()) {
                if (addedFirstInclude == true) {
                    sb.append(" OR ");
                }
                addedFirstInclude = true;
                sb.append("#INCLUDE(").append(field).append(", ").append(value).append(")");
            }
        }

        if (!includedValues.isEmpty() && !excludedValues.isEmpty()) {
            sb.append(" AND ");
        }

        boolean addedFirstExclude = false;
        for (Map.Entry<String,Set<String>> entry : excludedValues.entrySet()) {
            String field = entry.getKey();
            for (String value : entry.getValue()) {
                if (addedFirstExclude == true) {
                    sb.append(" AND ");
                }
                addedFirstExclude = true;
                sb.append("#EXCLUDE(").append(field).append(", ").append(value).append(")");
            }
        }

        if (sb.length() > 0) {
            query = "(" + query + ")" + " AND (" + sb + ")";
        }

        return super.parse(query);
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
}
