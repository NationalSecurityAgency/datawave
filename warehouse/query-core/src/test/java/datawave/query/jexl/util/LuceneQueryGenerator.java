package datawave.query.jexl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class LuceneQueryGenerator extends AbstractQueryGenerator {

    private int maxTermCount;
    private int currentTermCount;
    private int reservedCapacity;
    private int reservedNegatedCapacity;

    private String prevSeparator;

    public LuceneQueryGenerator(Set<String> fields, Set<String> values) {
        super(fields, values);
    }

    @Override
    public String getQuery() {
        return getQuery(5);
    }

    @Override
    public String getQuery(int minTerms, int maxTerms) {
        Preconditions.checkArgument(minTerms > 0, "minimum terms must be positive: " + minTerms);
        Preconditions.checkArgument(minTerms < maxTerms, "minTerms must be less than maxTerms: " + minTerms + " " + maxTerms);
        int numTerms = minTerms + random.nextInt(maxTerms - minTerms);
        return getQuery(numTerms);
    }

    @Override
    public String getQuery(int numTerms) {
        Preconditions.checkArgument(numTerms > 0, "minimum terms must be positive: " + numTerms);
        maxTermCount = numTerms;

        currentTermCount = 0;
        sb.setLength(0);

        if (maxTermCount == 1) {
            negationsEnabled = false;
            buildNode();
        } else {
            if (negationsEnabled) {
                reserveNegatedCapacity();
            }
            buildJunction();
        }

        // lucene wants negated terms at the end
        if (negationsEnabled) {
            addNegatedTerms();
        }

        return sb.toString();
    }

    private void reserveNegatedCapacity() {
        int reserved = random.nextInt(maxTermCount);
        maxTermCount -= reserved;
        reservedNegatedCapacity += reserved;
    }

    private void addNegatedTerms() {
        for (int i = 0; i < reservedNegatedCapacity; i++) {
            sb.append(" NOT ");
            buildLeaf();
        }
    }

    @Override
    protected void buildNode() {
        int remainingCapacity = maxTermCount - currentTermCount;

        int index;
        if ((remainingCapacity - reservedCapacity) < 3) {
            index = 1;
        } else {
            index = random.nextInt(2);
        }

        // need to take into account minimum term count. at a zero count we need to start with a junction

        switch (index) {
            case 0:
                buildJunction();
                break;
            case 1:
                buildLeaf();
                break;
            default:
                throw new IllegalStateException("Could not build term for index");
        }
    }

    @Override
    protected void buildJunction() {
        String separator = getSeparator();
        currentTermCount++;

        // calculate the number of child nodes given the remaining capacity.

        int children = 2; // minimum nodes for a junction
        reservedCapacity += 2; // reserve those child nodes

        int remainingCapacity = maxTermCount - (currentTermCount + reservedCapacity);

        // need to figure minimum and maximum term count into this.
        if (remainingCapacity > 0) {
            children += random.nextInt(remainingCapacity);
        }

        sb.append("(");
        for (int i = 0; i < children; i++) {
            buildNode();
            if (i < children - 1) {
                sb.append(separator);
            }
        }
        sb.append(")");
    }

    private String getSeparator() {
        String andSeparator = " AND ";
        String orSeparator = " OR ";

        if (!unionsEnabled) {
            return andSeparator;
        } else if (!intersectionsEnabled) {
            return orSeparator;
        }

        if (prevSeparator == null) {
            boolean intersecting = random.nextBoolean();
            prevSeparator = intersecting ? andSeparator : orSeparator;
        } else if (prevSeparator.equals(andSeparator)) {
            prevSeparator = orSeparator;
        } else {
            prevSeparator = andSeparator;
        }

        return prevSeparator;
    }

    @Override
    protected void buildLeaf() {
        NodeType type = getNextNodeType();
        switch (type) {
            case EQ:
                buildEqNode();
                break;
            case ER:
                buildErNode();
                break;
            case FILTER_FUNCTION:
                buildFilterFunction();
                break;
            case CONTENT_FUNCTION:
                buildContentFunction();
                break;
            case GROUPING_FUNCTION:
                buildGroupingFunction();
                break;
            default:
                throw new IllegalStateException("Error building leaf of type: " + type);
        }
    }

    private NodeType getNextNodeType() {
        NodeType type = null;
        while (type == null) {
            int index = random.nextInt(5);
            switch (index) {
                case 0:
                    type = NodeType.EQ;
                    break;
                case 1:
                    type = NodeType.ER;
                    break;
                case 2:
                    type = NodeType.FILTER_FUNCTION;
                    break;
                case 3:
                    type = NodeType.CONTENT_FUNCTION;
                    break;
                case 4:
                    type = NodeType.GROUPING_FUNCTION;
                    break;
                default:
                    throw new IllegalStateException("Could not get next node type for index: " + index);
            }

            if (!regexEnabled && type.equals(NodeType.ER)) {
                type = null;
            }

            if (!filterFunctionsEnabled && type != null && type.equals(NodeType.FILTER_FUNCTION)) {
                type = null;
            }

            if (!contentFunctionsEnabled && type != null && type.equals(NodeType.CONTENT_FUNCTION)) {
                type = null;
            }

            if (!groupingFunctionsEnabled && type != null && type.equals(NodeType.GROUPING_FUNCTION)) {
                type = null;
            }
        }
        return type;
    }

    private void buildEqNode() {
        incrementCounts();
        sb.append(getField()).append(":").append(getValue());
    }

    private void buildErNode() {
        incrementCounts();
        sb.append(getField()).append(":");
        int index = random.nextInt(3);
        switch (index) {
            case 0:
                // leading regex
                sb.append('*').append(getValue());
                break;
            case 1:
                // trailing regex
                sb.append(getValue()).append('*');
                break;
            case 2:
            default:
                // double ended regex
                sb.append('*').append(getValue()).append('*');
        }
    }

    private void buildFilterFunction() {
        incrementCounts();
        int index = random.nextInt(11);
        int count;
        switch (index) {
            case 0:
                // include
                sb.append("#INCLUDE(").append(getField()).append(", '").append(getValue()).append("')");
                break;
            case 1:
                // include multi-field
                sb.append("#INCLUDE(");
                count = 1 + random.nextInt(3);
                for (int i = 0; i < count; i++) {
                    sb.append(getField()).append(", ").append(getValue());
                    if (i < count - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
                break;
            case 2:
                // include multi-field, AND
                sb.append("#INCLUDE(AND, ");
                count = 1 + random.nextInt(3);
                for (int i = 0; i < count; i++) {
                    sb.append(getField()).append(", ").append(getValue());
                    if (i < count - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
                break;
            case 3:
                // include multi-field OR
                sb.append("#INCLUDE(OR, ");
                count = 1 + random.nextInt(3);
                for (int i = 0; i < count; i++) {
                    sb.append(getField()).append(", ").append(getValue());
                    if (i < count - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
                break;
            case 4:
                // exclude
                sb.append("#EXCLUDE(").append(getField()).append(", '").append(getValue()).append("')");
                break;
            case 5:
                // exclude multi-field
                sb.append("#EXCLUDE(");
                count = 1 + random.nextInt(3);
                for (int i = 0; i < count; i++) {
                    sb.append(getField()).append(", ").append(getValue());
                    if (i < count - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
                break;
            case 6:
                // exclude multi-field AND
                sb.append("#EXCLUDE(AND, ");
                count = 1 + random.nextInt(3);
                for (int i = 0; i < count; i++) {
                    sb.append(getField()).append(", ").append(getValue());
                    if (i < count - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
                break;
            case 7:
                // exclude multi-field OR
                sb.append("#EXCLUDE(OR, ");
                count = 1 + random.nextInt(3);
                for (int i = 0; i < count; i++) {
                    sb.append(getField()).append(", ").append(getValue());
                    if (i < count - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
                break;
            case 8:
                // is null
                sb.append("#ISNULL(").append(getField()).append(")");
                break;
            case 9:
                // is not null
                sb.append("#ISNOTNULL(").append(getField()).append(")");
                break;
            case 10:
                // occurrence
                sb.append("#OCCURRENCE(").append(getField()).append(", >, 1)");
                break;
            default:
                throw new IllegalStateException("could not build filter function for index: " + index);
        }
    }

    private void buildContentFunction() {
        incrementCounts();
        int index = random.nextInt(4);
        List<String> values;
        switch (index) {
            case 0:
                // unfielded content phrase function
                values = getValues(3);
                sb.append("\"").append(Joiner.on(' ').join(values)).append("\"");
                break;
            case 1:
                // fielded content phrase function
                values = getValues(3);
                sb.append(getField()).append(":\"").append(Joiner.on(' ').join(values)).append("\"");
                break;
            case 2:
                // unfielded content within function
                values = getValues(3);
                sb.append("\"").append(Joiner.on(' ').join(values)).append("\"~5");
                break;
            case 3:
                // fielded content within function
                values = getValues(3);
                sb.append(getField()).append("\"").append(Joiner.on(' ').join(values)).append("\"~5");
                break;
            default:
                // content:adjacent??
                throw new IllegalStateException("could not build content function for index: " + index);
        }
    }

    private void buildGroupingFunction() {
        int index = random.nextInt(2);
        switch (index) {
            case 0:
                // grouping matches in group
                sb.append("#MATCHES_IN_GROUP(");
                sb.append(getField()).append(", ").append(getValue()).append(", ");
                sb.append(getField()).append(", ").append(getValue()).append(")");
                break;
            case 1:
                // grouping matches in group left
                sb.append("#MATCHES_IN_GROUP_LEFT(");
                sb.append(getField()).append(", ").append(getValue()).append(", ");
                sb.append(getField()).append(", ").append(getValue()).append(")");
                break;
            default:
                throw new IllegalStateException("could not build grouping function for index: " + index);
        }
    }

    private void incrementCounts() {
        currentTermCount++;
        if (reservedCapacity > 0) {
            reservedCapacity--;
        }
    }

    private String getField() {
        int index = random.nextInt(fields.size());
        return fields.get(index);
    }

    private List<String> getValues(int count) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(getValue());
        }
        return values;
    }

    private String getValue() {
        int index = random.nextInt(values.size());
        return values.get(index);
    }
}
