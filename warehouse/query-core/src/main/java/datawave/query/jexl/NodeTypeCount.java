package datawave.query.jexl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.commons.jexl3.parser.Node;
import org.apache.commons.lang.builder.ToStringBuilder;

import datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType;

public class NodeTypeCount {

    /**
     * The map of node types to their respective counts.
     */
    private final Map<String,Integer> typeTotals = new HashMap<>();

    /**
     * Increment the count for the specified node type by 1.
     *
     * @param type
     *            the node type
     */
    public void increment(Class<? extends Node> type) {
        typeTotals.compute(type.getName(), (key, val) -> (val == null) ? 1 : val + 1);
    }

    /**
     * Increment the count for the specified node type by 1.
     *
     * @param type
     *            the marker type
     */
    public void increment(MarkerType type) {
        typeTotals.compute(type.getLabel(), (key, val) -> (val == null) ? 1 : val + 1);
    }

    /**
     * Return the total number of times the specified node type was found.
     *
     * @param type
     *            the node type
     * @return the total number
     */
    public int getTotal(Class<? extends Node> type) {
        return typeTotals.getOrDefault(type.getName(), 0);
    }

    /**
     * Return the total number of times the specified node type was found.
     *
     * @param type
     *            the node type
     * @return the total number
     */
    public int getTotal(MarkerType type) {
        return typeTotals.getOrDefault(type.getLabel(), 0);
    }

    /**
     * Return the total number of nodes that have been found.
     *
     * @return the total number of nodes
     */
    public long getTotalNodes() {
        return typeTotals.values().stream().reduce(0, Integer::sum);
    }

    /**
     * Return the total number of distinct types that have been found.
     *
     * @return the total distinct types
     */
    public int getTotalDistinctTypes() {
        return typeTotals.size();
    }

    /**
     * Return whether or not at least one of the specified node types was found.
     *
     * @param type
     *            the node type
     * @return true if the specified node type was found, or false otherwise
     */
    public boolean isPresent(Class<? extends Node> type) {
        return typeTotals.containsKey(type.getName());
    }

    /**
     * Return whether or not at least one of the specified node types was found.
     *
     * @param type
     *            the node type
     * @return true if the specified node type was found, or false otherwise
     */
    public boolean isPresent(MarkerType type) {
        return typeTotals.containsKey(type.getLabel());
    }

    @SuppressWarnings("unchecked")
    private boolean isPresent(Object type) {
        if (type instanceof Class && Node.class.isAssignableFrom((Class<?>) type)) {
            return isPresent((Class<? extends Node>) type);
        } else if (type instanceof MarkerType) {
            return isPresent((MarkerType) type);
        }
        return false;
    }

    /**
     * Return true if any of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    @SafeVarargs
    public final boolean hasAny(Class<? extends Node>... types) {
        return hasAny(Arrays.stream(types));
    }

    /**
     * Return true if any of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    @SafeVarargs
    public final boolean hasAny(MarkerType... types) {
        return hasAny(Arrays.stream(types));
    }

    /**
     * Return true if any of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    public boolean hasAny(Collection<?> types) {
        return hasAny(types.stream());
    }

    /**
     * Return true if any of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    public boolean hasAny(Stream<?> types) {
        return types.anyMatch(this::isPresent);
    }

    /**
     * Return true if all of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    @SafeVarargs
    public final boolean hasAll(Class<? extends Node>... types) {
        return hasAll(Arrays.stream(types));
    }

    /**
     * Return true if all of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    public final boolean hasAll(MarkerType... types) {
        return hasAll(Arrays.stream(types));
    }

    /**
     * Return true if all of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    public boolean hasAll(Collection<?> types) {
        return hasAll(types.stream());
    }

    /**
     * Return true if all of the specified node types were found.
     *
     * @param types
     *            the node types
     * @return true if any of the types were found, or false otherwise
     */
    public boolean hasAll(Stream<?> types) {
        return types.allMatch(this::isPresent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeTypeCount count = (NodeTypeCount) o;
        return Objects.equals(typeTotals, count.typeTotals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeTotals);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("typeTotals", typeTotals).toString();
    }

    /**
     * Return a formatted line-by-line string of the node type counts in alphabetical order.
     *
     * @return the formatted string
     */
    public String toPrettyString() {
        StringBuilder sb = new StringBuilder();
        // @formatter:off
        typeTotals.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach((entry) -> sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"));
        // @formatter:on
        return sb.deleteCharAt(sb.lastIndexOf("\n")).toString();
    }
}
