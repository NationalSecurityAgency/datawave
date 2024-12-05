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
 * Represents options for a summary that have been specified within an #SUMMARY_SIZE function. An instance of {@link SummarySize} can easily be captured as a
 * parameter string using {@link SummarySize#toString()}, and transformed back into a {@link SummarySize} instance via {@link SummarySize#from(String)}.
 */
public class SummarySize implements Serializable {

    private static final long serialVersionUID = 6769159729743311079L;

    private static final Logger log = LoggerFactory.getLogger(SummarySize.class);

    public static final String SIZE_PARAMETER = "SIZE";
    public static final String VIEWS_PARAMETER = "VIEWS";

    private static final int DEFAULT_SIZE = 150;

    private int summarySize;
    private ArrayList<String> contentNamesList;
    private boolean only;

    public SummarySize() {
        summarySize = 0;
        contentNamesList = new ArrayList<>();
        only = false;
    }

    /**
     * Returns a new {@link SummarySize} parsed from the string. The provided string is expected to have the format returned by {@link SummarySize#toString()}.
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link SummarySize} will be returned.</li>
     * <li>Given {@code SIZE:50/ONLY/NAMES:CONTENT1,CONTENT2}, an {@link SummarySize} will be returned with a size of 50 (size is number of characters), only
     * using the specified content names, and list of content names of (CONTENT1, CONTENT2).
     * <li>Given malformed input, will return an {@link SummarySize} with a size of 150.</li>
     * </ul>
     *
     * @param string
     *            the string to parse
     * @return the parsed {@link SummarySize}
     */
    @JsonCreator
    public static SummarySize from(String string) {
        if (string == null) {
            return null;
        }
        // Strip whitespaces.
        string = PhraseIndexes.whitespacePattern.matcher(string).replaceAll("");

        SummarySize summarySize = new SummarySize();

        if (string.isBlank()) {
            summarySize.summarySize = DEFAULT_SIZE;
            return summarySize;
        }

        try {
            String[] parameterParts = string.split(Constants.FORWARD_SLASH);

            for (String parameterPart : parameterParts) {
                String[] parts = parameterPart.split(Constants.COLON);
                if (parts[0].equalsIgnoreCase(SIZE_PARAMETER)) {
                    summarySize.summarySize = Integer.parseInt(parts[1]);
                } else if (parts[0].equalsIgnoreCase("ONLY")) {
                    summarySize.only = true;
                } else if (parts[0].equalsIgnoreCase(VIEWS_PARAMETER)) {
                    String[] names = parts[1].split(Constants.COMMA);
                    for (String name : names) {
                        summarySize.contentNamesList.add(name.toUpperCase());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Unable to parse summary size string: {}", string, e);
            return new SummarySize();
        }

        return summarySize;
    }

    /**
     * Returns a copy of the given {@link SummarySize}
     *
     * @param other
     *            the instance to copy
     * @return the copy
     */
    public static SummarySize copyOf(SummarySize other) {
        if (other == null) {
            return null;
        }
        SummarySize summarySize = new SummarySize();
        summarySize.summarySize = other.summarySize;
        summarySize.contentNamesList = new ArrayList<>(other.contentNamesList);
        summarySize.only = other.only;
        return summarySize;
    }

    public int getSummarySize() {
        return summarySize;
    }

    public boolean onlyListedContents() {
        return only;
    }

    /**
     * Replace a content name with another content name
     *
     * @param contentName
     *            the one to replace
     * @param replacement
     *            the one to replace the other
     */
    public void replace(String contentName, String replacement) {
        int index = contentNamesList.indexOf(contentName);
        if (index != -1) {
            contentNamesList.set(index, replacement);
        }
    }

    /**
     * Return whether this {@link SummarySize} content names list is empty.
     *
     * @return true if empty, or false otherwise
     */
    public boolean isEmpty() {
        return contentNamesList.isEmpty();
    }

    public String contentNamesListToString() {
        if (contentNamesList.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (String contentName : contentNamesList) {
            sb.append(contentName).append(Constants.COMMA);
        }
        return sb.substring(0, sb.length() - 1);
    }

    public static String[] contentNamesListFromString(String string) {
        return string.split(Constants.COMMA);
    }

    /**
     * Returns this {@link SummarySize} as a formatted string that can later be parsed back into a {@link SummarySize} using {@link SummarySize#from(String)}.
     * This is also what will be used when serializing a {@link SummarySize} to JSON/XML. The string will have the format
     * {@code SIZE:size/[only]/[NAMES:contentName1, contentName2, ....]}.
     *
     * @return a formatted string
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(SIZE_PARAMETER).append(":").append(summarySize);
        if (only) {
            sb.append("/").append("ONLY");
        }
        if (!contentNamesList.isEmpty()) {
            sb.append("/").append(VIEWS_PARAMETER).append(":");
            for (String contentName : contentNamesList) {
                sb.append(contentName).append(Constants.COMMA);
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
        SummarySize that = (SummarySize) o;
        return Objects.equals(summarySize, that.summarySize) && Objects.equals(contentNamesList, that.contentNamesList) && Objects.equals(only, that.only);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summarySize, contentNamesList, only);
    }
}
