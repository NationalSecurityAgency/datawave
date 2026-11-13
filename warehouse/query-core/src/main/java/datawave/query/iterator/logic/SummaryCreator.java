package datawave.query.iterator.logic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Formats summaries from decoded, bounded content prefixes. Callers apply their content-length limit before constructing this class.
 */
public class SummaryCreator {
    private final List<String> viewSummaryOrder;
    private final Map<String,String> foundContent;

    public SummaryCreator(List<String> viewSummaryOrder, Map<String,String> foundContent) {
        this.viewSummaryOrder = viewSummaryOrder;
        this.foundContent = foundContent;
    }

    /**
     * this method attempts to create a summary out of the found views
     *
     * @return the created summary
     */
    public String createSummary() {
        // check each potential view name we could make summaries for
        for (String name : viewSummaryOrder) {
            if (name.endsWith("*")) {
                // strip wildcard from view name
                name = name.substring(0, name.length() - 1);

                String endingWildcardSummary = getEndingWildcardSummary(name, foundContent);
                if (endingWildcardSummary != null) {
                    return endingWildcardSummary;
                }
            } else {
                String simpleSummary = getSimpleSummary(name, foundContent);
                if (simpleSummary != null) {
                    return simpleSummary;
                }
            }
        }
        return null;
    }

    /** for matching and creating summaries when view names have trailing wildcards */
    private static String getEndingWildcardSummary(String currentViewName, Map<String,String> foundContent) {
        // if we have a view name that matches the list...
        Map<String,String> summaries = new HashMap<>();
        for (Map.Entry<String,String> entry : foundContent.entrySet()) {
            if (entry.getKey().startsWith(currentViewName)) {
                summaries.put(entry.getKey(), entry.getValue());
            }
        }
        if (!summaries.isEmpty()) {
            // return the view name and summary separated by a new line character
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String,String> entry : summaries.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            return sb.toString().trim();
        }
        return null;
    }

    /** a straight-up match between view names */
    private static String getSimpleSummary(String currentViewName, Map<String,String> foundContent) {
        for (Map.Entry<String,String> entry : foundContent.entrySet()) {
            if (entry.getKey().equals(currentViewName)) {
                return currentViewName + ": " + entry.getValue();
            }
        }
        return null;
    }

}
