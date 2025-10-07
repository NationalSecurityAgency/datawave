package datawave.query.tables;

import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

public class QueryMacroFunction implements Function<String,String> {

    private Map<String,String> queryMacros;

    public Map<String,String> getQueryMacros() {
        return queryMacros;
    }

    public void setQueryMacros(Map<String,String> queryMacros) {
        this.queryMacros = queryMacros;
    }

    @Override
    public String apply(String query) {
        for (String key : queryMacros.keySet()) {
            if (query.contains(key)) {
                // blah blah FOO:selector blah blah ".*("+key+":([\\S]+)).*"
                String patternOne = ".*(" + key + ":([\\S]+)).*";
                // blah blah FOO(selector1,selector2,...) blah blah ".*("+key+"\\(([\\S]+\\))).*"
                String patternTwo = ".*(" + key + "\\(([\\S]+)\\)).*";
                Pattern pattern = Pattern.compile(patternTwo);
                Matcher matcher = pattern.matcher(query);
                while (matcher.find()) {
                    String replacementPart = queryMacros.get(key);
                    Map<String,String> selectorMap = getSelectorMap(matcher.group(2));
                    // replace the placeholders with the selectors
                    for (Entry<String,String> entry : selectorMap.entrySet()) {
                        replacementPart = replacementPart.replaceAll(entry.getKey(), entry.getValue());
                    }
                    // then replace the macro with the replacement part
                    query = query.replace(matcher.group(1), replacementPart);
                    matcher = pattern.matcher(query);
                }
            }
        }
        return query;
    }

    private Map<String,String> getSelectorMap(String macroArguments) {
        Map<String,String> selectorMap = Maps.newHashMap();
        int i = 0;
        for (String selector : Splitter.on(',').omitEmptyStrings().trimResults().split(macroArguments)) {
            selectorMap.put("\\$" + i, selector);
            i++;
        }
        return selectorMap;
    }

    @Override
    public String toString() {
        return "QueryMacroFunction [queryMacros=" + queryMacros + "]";
    }

}
