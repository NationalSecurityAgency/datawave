package datawave.core.query.search;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Keeps track of the query term, location to be searched, and also deals with wildcards.
 */
public class WildcardFieldedFilter extends FieldedTerm {
    public static enum BooleanType {
        AND, OR
    }

    private static Logger log = Logger.getLogger(WildcardFieldedFilter.class.getName());
    private List<Pattern> selectorRegexList = new ArrayList<>();
    private List<String> fieldList = new ArrayList<>();
    private Boolean includeIfMatch = null;
    private BooleanType type;

    public WildcardFieldedFilter(boolean includeIfMatch, BooleanType type) {
        super();
        this.includeIfMatch = includeIfMatch;
        this.type = type;
        setFilterOnly(true);
    }

    public static Pattern convertToRegex(String s) {
        int flags = Pattern.DOTALL;
        return Pattern.compile(s, flags);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("filter(");
        sb.append(includeIfMatch);
        sb.append(", ");
        sb.append(type);
        sb.append(", ");
        for (int x = 0; x < fieldList.size(); x++) {
            if (x > 0) {
                sb.append(", ");
            }
            sb.append(fieldList.get(x));
            sb.append(", ");
            sb.append(selectorRegexList.get(x));
        }
        sb.append(")");
        return sb.toString();
    }

    public void addCondition(String field, String selector) {
        fieldList.add(field);
        selectorRegexList.add(convertToRegex(selector.toLowerCase()));
    }
}
