package datawave.query.attributes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import datawave.query.Constants;
import datawave.query.postprocessing.tf.PhraseIndexes;

/**
 * Represents options for a summary that have been specified within an #SUMMARY_SIZE function. An instance of {@link SummaryOptions} can easily be captured as a
 * parameter string using {@link SummaryOptions#toString()}, and transformed back into a {@link SummaryOptions} instance via
 * {@link SummaryOptions#from(String)}.
 */
public class SummaryOptions implements Serializable {

    private static final long serialVersionUID = 6769159729743311079L;

    private static final Logger log = LoggerFactory.getLogger(SummaryOptions.class);

    private static final String SIZE_PARAMETER = "SIZE";
    private static final String VIEWS_PARAMETER = "VIEWS";
    private static final String ONLY_PARAMETER = "ONLY";

    // TODO: until it works without arguments
    private static final String TRUE = "TRUE";

    public static final int DEFAULT_SIZE = 150;

    private int summarySize;
    private ArrayList<String> viewNamesList;
    /**
     * When set, we will only use the view names passed in to the function to attempt to make summaries from (clears out default list).
     * <p>
     * </p>
     * When this is set, the user should also pass in a list of view names or else there is no chance of summaries being returned.
     */
    private boolean onlyListedViews;

    public SummaryOptions() {
        summarySize = 0;
        viewNamesList = new ArrayList<>();
        onlyListedViews = false;
    }

    /**
     * Returns a new {@link SummaryOptions} parsed from the string. The provided string is expected to have the format returned by
     * {@link SummaryOptions#toString()}.
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, a {@link SummaryOptions} with a size of DEFAULT_SIZE (currently 150) will be returned.</li>
     * <li>Given {@code SIZE:50/ONLY/VIEWS:CONTENT1,CONTENT2}, an {@link SummaryOptions} will be returned with a size of 50 (size is number of characters), only
     * using the specified view names, and list of view names of (CONTENT1, CONTENT2).
     * <li>Given malformed input, will return an empty {@link SummaryOptions}.</li>
     * </ul>
     *
     * @param string
     *            the string to parse
     * @return the parsed {@link SummaryOptions}
     */
    @JsonCreator
    public static SummaryOptions from(String string) {
        if (string == null) {
            return null;
        }
        // Strip whitespaces.
        string = PhraseIndexes.whitespacePattern.matcher(string).replaceAll("");

        SummaryOptions summaryOptions = new SummaryOptions();

        // TODO: this is in here for when we can make accepting functions with no arguments work
        // if passed no parameters, return of summary of default size
        if (string.isBlank()) {
            summaryOptions.summarySize = DEFAULT_SIZE;
            return summaryOptions;
        }

        try {
            // split on / to get the separate options
            String[] parameterParts = string.split(Constants.FORWARD_SLASH);

            // go through each option and try to set them
            for (String parameterPart : parameterParts) {
                // for options that are "key:value", split on colon to get the key
                String[] parts = parameterPart.split(Constants.COLON);
                // if we have the "size" option...
                if (parts[0].equalsIgnoreCase(SIZE_PARAMETER)) {
                    int size = Integer.parseInt(parts[1]);
                    if (size == 0) {
                        return new SummaryOptions();
                    }
                    summaryOptions.summarySize = size;
                }
                // if we have the "only" option...
                else if (parts[0].equalsIgnoreCase(ONLY_PARAMETER)) {
                    summaryOptions.onlyListedViews = true;
                }
                // if we have the "views" option...
                else if (parts[0].equalsIgnoreCase(VIEWS_PARAMETER)) {
                    // the view names are split by commas. split them, uppercase them, then add the to the list.
                    String[] names = parts[1].split(Constants.COMMA);
                    for (String name : names) {
                        summaryOptions.viewNamesList.add(name.toUpperCase());
                    }
                }
            }
            // this part also happens to make it so if people put anything random in the function and don't set any options, it will still return a summary of
            // default size
            // if size was not specified, make it DEFAULT_SIZE
            if (summaryOptions.summarySize == 0) {
                summaryOptions.summarySize = DEFAULT_SIZE;
            }
        } catch (NumberFormatException e) {
            log.warn("Unable to parse summary size string, returning empty SummaryOptions: {}", string, e);
            return new SummaryOptions();
        }

        return summaryOptions;
    }

    /**
     * Returns a copy of the given {@link SummaryOptions}
     *
     * @param other
     *            the instance to copy
     * @return the copy
     */
    public static SummaryOptions copyOf(SummaryOptions other) {
        if (other == null) {
            return null;
        }
        SummaryOptions summaryOptions = new SummaryOptions();
        summaryOptions.summarySize = other.summarySize;
        summaryOptions.viewNamesList = new ArrayList<>(other.viewNamesList);
        summaryOptions.onlyListedViews = other.onlyListedViews;
        return summaryOptions;
    }

    public int getSummarySize() {
        return summarySize;
    }

    public boolean onlyListedViews() {
        return onlyListedViews;
    }

    /**
     * Replace a view name with another view name
     *
     * @param viewName
     *            the one to replace
     * @param replacement
     *            the one to replace the other
     */
    public void replace(String viewName, String replacement) {
        int index = viewNamesList.indexOf(viewName);
        if (index != -1) {
            viewNamesList.set(index, replacement);
        }
    }

    /**
     * Return whether this {@link SummaryOptions} view names list is empty.
     *
     * @return true if empty, or false otherwise
     */
    public boolean isEmpty() {
        return viewNamesList.isEmpty();
    }

    public String viewNamesListToString() {
        if (viewNamesList.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (String viewName : viewNamesList) {
            sb.append(viewName).append(Constants.COMMA);
        }
        return sb.substring(0, sb.length() - 1);
    }

    public static String[] viewNamesListFromString(String string) {
        return string.split(Constants.COMMA);
    }

    /**
     * Returns this {@link SummaryOptions} as a formatted string that can later be parsed back into a {@link SummaryOptions} using
     * {@link SummaryOptions#from(String)}. This is also what will be used when serializing a {@link SummaryOptions} to JSON/XML. The string will have the
     * format {@code SIZE:size/[only]/[NAMES:contentName1, contentName2, ....]}.
     *
     * @return a formatted string
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(SIZE_PARAMETER).append(":").append(summarySize);
        if (onlyListedViews) {
            sb.append("/").append(ONLY_PARAMETER);
        }
        if (!viewNamesList.isEmpty()) {
            sb.append("/").append(VIEWS_PARAMETER).append(":");
            for (String viewName : viewNamesList) {
                sb.append(viewName).append(Constants.COMMA);
            }
            return sb.substring(0, sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SummaryOptions that = (SummaryOptions) o;
        return Objects.equals(summarySize, that.summarySize) && Objects.equals(viewNamesList, that.viewNamesList)
                        && Objects.equals(onlyListedViews, that.onlyListedViews);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summarySize, viewNamesList, onlyListedViews);
    }
}
