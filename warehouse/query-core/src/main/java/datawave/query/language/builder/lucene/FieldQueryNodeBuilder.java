package datawave.query.language.builder.lucene;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

import datawave.query.language.tree.AdjNode;
import datawave.query.language.tree.SelectorNode;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.search.TermQuery;

/**
 * Builds a {@link TermQuery} object from a {@link FieldQueryNode} object.
 */
@Deprecated
public class FieldQueryNodeBuilder implements QueryBuilder {

    private static final String WHITE_SPACE_ESCAPE_STRING = "~~";
    public static final String SPACE = " ";

    public datawave.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        datawave.query.language.tree.QueryNode returnNode = null;

        if (queryNode instanceof QuotedFieldQueryNode) {
            List<datawave.query.language.tree.QueryNode> childrenList = new ArrayList<>();
            FieldQueryNode quotedFieldNode = (QuotedFieldQueryNode) queryNode;
            String field = quotedFieldNode.getFieldAsString();
            String selector = quotedFieldNode.getTextAsString();

            // check if spaces were escaped
            FieldQueryNode fieldQueryNode = (QuotedFieldQueryNode) queryNode;
            UnescapedCharSequence origChars = (UnescapedCharSequence) fieldQueryNode.getText();
            int nextWordStart = 0;
            ArrayList<String> words = new ArrayList<>();

            for (int x = 0; x < origChars.length(); x++) {
                if (origChars.charAt(x) == ' ' && !origChars.wasEscaped(x)) {
                    words.add(selector.substring(nextWordStart, x));
                    nextWordStart = x + 1;
                } else if (x == origChars.length() - 1) {
                    // last word in the list
                    words.add(selector.substring(nextWordStart));
                }
            }

            replaceEscapeCharsWithSpace(words);

            for (String s : words) {
                String currWord = s.trim();
                if (!currWord.isEmpty()) {
                    if (field == null || field.isEmpty()) {
                        childrenList.add(new SelectorNode("", currWord));
                    } else {
                        childrenList.add(new SelectorNode(field, currWord));
                    }
                }
            }

            datawave.query.language.tree.QueryNode[] childrenArray = new datawave.query.language.tree.QueryNode[childrenList.size()];
            childrenList.toArray(childrenArray);
            if (childrenArray.length == 1) {
                // if only one term in quotes, just use a SelectorNode
                returnNode = childrenArray[0];
            } else {
                // if more than one term in quotes, use an AdjNode
                returnNode = new AdjNode((childrenArray.length - 1), childrenArray);
            }

        } else if (queryNode instanceof FuzzyQueryNode) {
            throw new UnsupportedOperationException(queryNode.getClass().getName() + " not implemented");
        } else {
            // queryNode instanceof WildcardQueryNode
            // queryNode instanceof QuotedFieldQueryNode
            // queryNode instanceof FieldQueryNode

            FieldQueryNode fieldNode = (FieldQueryNode) queryNode;
            String field = fieldNode.getFieldAsString();

            // Keep the escape characters for *, ?, and \ so that we can determine which are wildcards and which are escaped
            StringBuilder sb = new StringBuilder();
            UnescapedCharSequence seq = (UnescapedCharSequence) fieldNode.getText();
            for (int x = 0; x < seq.length(); x++) {
                char c = seq.charAt(x);
                if (seq.wasEscaped(x) && (c == '*' || c == '?' || c == '\\')) {
                    sb.append("\\").append(c);
                } else {
                    sb.append(c);
                }
            }
            String selector = sb.toString();

            selector = replaceEscapeCharsWithSpace(selector, true);

            returnNode = new SelectorNode(field, selector);
        }

        return returnNode;
    }

    private void replaceEscapeCharsWithSpace(ArrayList<String> words) {
        for (int i = 0; i < words.size(); i++) {
            words.set(i, replaceEscapeCharsWithSpace(words.get(i), false));
        }
    }

    private String replaceEscapeCharsWithSpace(String s, boolean shouldTrimIfFound) {
        boolean shouldTrim = shouldTrimIfFound && s.contains(WHITE_SPACE_ESCAPE_STRING);
        String result = s.replaceAll(WHITE_SPACE_ESCAPE_STRING, SPACE);
        return shouldTrim ? result.trim() : result;// for review: do we really want to trim whitespace (and only if found)? maybe we should trim first?
    }

}
