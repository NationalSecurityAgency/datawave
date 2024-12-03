package datawave.query.jexl.util;

import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * A random query generator useful for testing visitors against randomized inputs
 * <p>
 * <strong>feature available via toggle</strong>
 * <ul>
 * <li>negations</li>
 * <li>regex</li>
 * <li>filter functions</li>
 * <li>content functions</li>
 * <li>grouping functions</li>
 * </ul>
 *
 * Optionally limit functions to those without fields, with single fields, and/or multi fields
 */
public class JexlQueryGenerator extends AbstractQueryGenerator {

    private int maxTermCount;
    private int currentTermCount;
    private int reservedCapacity;

    private String prevSeparator;

    public JexlQueryGenerator(Set<String> fields, Set<String> values) {
        super(fields, values);
    }

    /**
     * Builds a query with a default size of five terms
     *
     * @return a random query string
     */
    public String getQuery() {
        return getQuery(5);
    }

    /**
     * Builds a query with variable size between the provided bounds
     *
     * @param minTerms
     *            the lower bound
     * @param maxTerms
     *            the upper bound
     * @return a random query tree
     */
    public String getQuery(int minTerms, int maxTerms) {
        Preconditions.checkArgument(minTerms > 0, "minimum terms must be positive: " + minTerms);
        Preconditions.checkArgument(minTerms < maxTerms, "minTerms must be less than maxTerms: " + minTerms + " " + maxTerms);
        validateOptions();
        int numTerms = minTerms + random.nextInt(maxTerms - minTerms);
        return getQuery(numTerms);
    }

    /**
     * Builds a random query of the specified size
     *
     * @param numTerms
     *            the number of terms in the query
     * @return a random query string
     */
    public String getQuery(int numTerms) {
        Preconditions.checkArgument(numTerms > 0, "minimum terms must be positive: " + numTerms);
        validateOptions();
        maxTermCount = numTerms;

        currentTermCount = 0;
        sb.setLength(0);

        if (maxTermCount == 1) {
            buildNode();
        } else {
            buildJunction();
        }
        return sb.toString();
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
        String andSeparator = " && ";
        String orSeparator = " || ";

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
            case NE:
                buildNeNode();
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
            case EQ_NULL:
                buildNullEquality();
                break;
            case NE_NULL:
                buildNullNotEquals();
                break;
            default:
                throw new IllegalStateException("Error building leaf of type: " + type);
        }
    }

    private NodeType getNextNodeType() {
        NodeType type = null;
        while (type == null) {
            int index = random.nextInt(8);
            switch (index) {
                case 0:
                    type = NodeType.EQ;
                    break;
                case 1:
                    type = NodeType.NE;
                    break;
                case 2:
                    type = NodeType.ER;
                    break;
                case 3:
                    type = NodeType.FILTER_FUNCTION;
                    break;
                case 4:
                    type = NodeType.CONTENT_FUNCTION;
                    break;
                case 5:
                    type = NodeType.GROUPING_FUNCTION;
                    break;
                case 6:
                    type = NodeType.EQ_NULL;
                    break;
                case 7:
                    type = NodeType.NE_NULL;
                    break;
                default:
                    throw new IllegalStateException("Could not get next node type for index: " + index);
            }

            if (!negationsEnabled && type.equals(NodeType.NE)) {
                type = null;
            }

            if (!regexEnabled && type != null && type.equals(NodeType.ER)) {
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

            if (!nullLiteralsEnabled && type != null && type.equals(NodeType.EQ_NULL)) {
                type = null;
            }

            if (!nullLiteralsEnabled && type != null && type.equals(NodeType.NE_NULL)) {
                type = null;
            }
        }
        return type;
    }

    private void buildEqNode() {
        incrementCounts();
        String field = getField();
        String value = getValue();
        sb.append(field).append(" == '").append(value).append("'");
    }

    private void buildNeNode() {
        incrementCounts();
        String field = getField();
        String value = getValue();
        sb.append("!(").append(field).append(" == '").append(value).append("')");
    }

    private void buildErNode() {
        incrementCounts();
        String field = getField();
        String value = getValue();
        sb.append(field).append(" =~ '");
        int state = random.nextInt(3);
        switch (state) {
            case 0:
                // leading
                sb.append(".*").append(value);
                break;
            case 1:
                // trailing
                sb.append(value).append(".*");
                break;
            case 2:
            default:
                // double ended
                sb.append(".*").append(value).append(".*");

        }
        sb.append("'");
    }

    private void buildFilterFunction() {

        incrementCounts();
        String field = getField();
        String value = getValue();
        sb.append("filter:");

        int bound = 8;
        if (multiFieldedFunctionsEnabled) {
            bound += 6;
        }

        int index = random.nextInt(bound);
        String fields;
        switch (index) {
            case 0:
                // include regex, fielded
                sb.append("includeRegex(").append(field).append(",'").append(value).append(".*')");
                break;
            case 1:
                // exclude regex, fielded
                sb.append("excludeRegex(").append(field).append(",'").append(value).append(".*')");
                break;
            case 2:
                // isNull, single field
                // the RewriteNullFunctionsVisitor should remove these functions, but test them anyway
                sb.append("isNull(").append(field).append(")");
                break;
            case 3:
                // isNotNull, single field
                sb.append("isNotNull(").append(field).append(")");
                break;
            case 4:
                sb.append("betweenDates(").append(field).append(", '2024-01-01', '2024-01-05')");
                break;
            case 5:
                sb.append("betweenLoadDates(").append(field).append(", '20240101', '20240105', 'yyyyMMdd')");
                break;
            case 6:
                sb.append("matchesAtLeastCountOf(1,").append(field).append(",").append(getValue()).append(")");
                break;
            case 7:
                // occurrence
                sb.append("occurrence(").append(getField()).append(", '>', 1)");
                break;
            case 8:
                // compare function is actually two fields. A user could compare the values of the same field
                sb.append("compare(").append(field).append(", '>', 'ANY', ").append(getField()).append(")");
                break;
            case 9:
                // filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)
                sb.append("timeFunction(").append(field).append(",").append(getField()).append(",'-','>',2522880000000L)");
                break;
            case 10:
                // include regex, multi-fielded
                fields = "(" + getField() + " || " + getField() + ")";
                sb.append("includeRegex(").append(fields).append(",'").append(value).append(".*')");
                break;
            case 11:
                // exclude regex, multi-fielded
                fields = "(" + getField() + " || " + getField() + ")";
                sb.append("excludeRegex(").append(fields).append(",'").append(value).append(".*')");
                break;
            case 12:
                // isNull, multi-field
                // the RewriteNullFunctionsVisitor should remove these functions, but test them anyway
                fields = getField() + " || " + getField();
                sb.append("isNull(").append(fields).append(")");
                break;
            case 13:
                // isNotNull, multi-field
                fields = getField() + " || " + getField();
                sb.append("isNotNull(").append(fields).append(")");
                break;
            default:
                throw new IllegalStateException("should never get here. bound: " + bound + ", query: " + sb.toString());
        }
    }

    private void buildContentFunction() {
        incrementCounts();
        sb.append("content:");

        int minimum = 4;
        if (noFieldedFunctionsEnabled) {
            minimum = 0;
        }

        int bound = 4;
        if (multiFieldedFunctionsEnabled) {
            bound += 4;
        }

        int index = minimum + random.nextInt(bound);
        String fields;
        switch (index) {
            case 0:
                // phrase, no field
                sb.append("phrase(termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 1:
                // adjacent, no field
                sb.append("adjacent(termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 2:
                // within, no field
                sb.append("within(2, termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 3:
                // scored phrase, no field
                sb.append("scoredPhrase(-1.5, termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 4:
                // adjacent, fielded
                sb.append("adjacent(").append(getField()).append(", termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 5:
                // phrase, fielded
                sb.append("phrase(").append(getField()).append(", termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 6:
                // within, fielded
                sb.append("within(").append(getField()).append(", 2, termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 7:
                // scored phrase, fielded
                sb.append("scoredPhrase(").append(getField()).append(", -1.5, termOffsetMap, '").append(getValue()).append("', '").append(getValue())
                                .append("')");
                break;
            case 8:
                // adjacent, multi-fielded
                fields = "(" + getField() + " || " + getField() + ")";
                sb.append("adjacent(").append(fields).append(", termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 9:
                // phrase, multi-fielded
                fields = "(" + getField() + " || " + getField() + ")";
                sb.append("phrase(").append(fields).append(", termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 10:
                // within, multi-fielded
                fields = "(" + getField() + " || " + getField() + ")";
                sb.append("within(").append(fields).append(", 2, termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            case 11:
                // scored phrase, multi-fielded
                fields = "(" + getField() + " || " + getField() + ")";
                sb.append("scoredPhrase(").append(fields).append(", -1.5, termOffsetMap, '").append(getValue()).append("', '").append(getValue()).append("')");
                break;
            default:
                throw new IllegalStateException("Unhandled case");
        }
    }

    private void buildGroupingFunction() {
        incrementCounts();
        sb.append("f:");

        int bound = 5;
        if (multiFieldedFunctionsEnabled) {
            bound += 1;
        }

        int index = random.nextInt(bound);
        switch (index) {
            case 0:
                // sum
                sb.append("sum(").append(getField()).append(")");
                break;
            case 1:
                // count, single field
                sb.append("count(").append(getField()).append(")");
                break;
            case 2:
                // average
                sb.append("average(").append(getField()).append(")");
                break;
            case 3:
                // min
                sb.append("min(").append(getField()).append(")");
                break;
            case 4:
                // max
                sb.append("max(").append(getField()).append(")");
                break;
            case 5:
                // count, multi field
                sb.append("count(");
                int extraFields = 1 + random.nextInt(3);
                for (int i = 0; i < extraFields; i++) {
                    sb.append(getField());
                    if (i < extraFields - 1) {
                        sb.append(",");
                    }
                }
                sb.append(")");
                break;
            default:
                throw new IllegalStateException("unknown index: " + index);
        }
    }

    private void buildNullEquality() {
        incrementCounts();
        String field = getField();
        sb.append(field).append(" == null");
    }

    private void buildNullNotEquals() {
        incrementCounts();
        String field = getField();
        sb.append("!(").append(field).append(" == null)");
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

    private String getValue() {
        int index = random.nextInt(values.size());
        return values.get(index);
    }

}
