package datawave.query.iterator.logic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.query.table.parser.ContentKeyValueFactory;

/**
 * This class contains the functionality to generate summaries.
 * <p>
 * </p>
 * Just need to call "createSummary()" after creation.
 */
public class SummaryCreator {
    private final List<String> viewSummaryOrder;
    Map<String,byte[]> foundContent;
    int summarySize;

    public SummaryCreator(List<String> viewSummaryOrder, Map<String,byte[]> foundContent, int summarySize) {
        this.viewSummaryOrder = viewSummaryOrder;
        this.foundContent = foundContent;
        this.summarySize = summarySize;
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

                String endingWildcardSummary = getEndingWildcardSummary(name, foundContent, summarySize);
                if (endingWildcardSummary != null) {
                    return endingWildcardSummary;
                }
            } else {
                String simpleSummary = getSimpleSummary(name, foundContent, summarySize);
                if (simpleSummary != null) {
                    return simpleSummary;
                }
            }
        }
        return null;
    }

    /** for matching and creating summaries when view names have trailing wildcards */
    private static String getEndingWildcardSummary(String currentViewName, Map<String,byte[]> foundContent, int summarySize) {
        // if we have a view name that matches the list...
        Map<String,String> summaries = new HashMap<>();
        for (Map.Entry<String,byte[]> entry : foundContent.entrySet()) {
            if (entry.getKey().startsWith(currentViewName)) {
                // decode and decompress the content
                String summary = new String(ContentKeyValueFactory.decodeAndDecompressContent(entry.getValue()));
                // if the content is longer than the specified length, truncate it
                if (summary.length() > summarySize) {
                    summary = summary.substring(0, summarySize);
                }
                summaries.put(entry.getKey(), summary);
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
    private static String getSimpleSummary(String currentViewName, Map<String,byte[]> foundContent, int summarySize) {
        if (foundContent.containsKey(currentViewName)) {
            // decode and decompress the content
            String summary = new String(ContentKeyValueFactory.decodeAndDecompressContent(foundContent.get(currentViewName)));
            // if the content is longer than the specified length, truncate it
            if (summary.length() > summarySize) {
                summary = summary.substring(0, summarySize);
            }
            return currentViewName + ": " + summary;
        }
        return null;
    }
}
