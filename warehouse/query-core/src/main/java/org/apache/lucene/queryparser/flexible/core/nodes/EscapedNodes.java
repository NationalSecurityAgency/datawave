package org.apache.lucene.queryparser.flexible.core.nodes;

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/**
 *
 */
public class EscapedNodes {
    public static String getEscapedTerm(FieldQueryNode node, EscapeQuerySyntax escaper) {
        return node.getTermEscaped(escaper).toString();
    }
}
