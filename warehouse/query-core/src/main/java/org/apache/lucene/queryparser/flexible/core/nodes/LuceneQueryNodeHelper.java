package org.apache.lucene.queryparser.flexible.core.nodes;

import org.apache.commons.lang3.StringUtils;

/**
 * This class provides methods that are able to access protected members of classes from the apache lucene library.
 */
public class LuceneQueryNodeHelper {

    /**
     * Returns whether the given field is considered a default field for the given node.
     *
     * @param node
     *            the node
     * @param field
     *            the field
     * @return true if the given field is null or blank, or considered a default field by the node, or false otherwise
     */
    public static boolean isDefaultField(QueryNode node, CharSequence field) {
        if (StringUtils.isBlank(field)) {
            return true;
        }
        if (node instanceof QueryNodeImpl) {
            return ((QueryNodeImpl) node).isDefaultField(field);
        } else {
            return false;
        }
    }

    private LuceneQueryNodeHelper() {
        throw new UnsupportedOperationException();
    }
}
