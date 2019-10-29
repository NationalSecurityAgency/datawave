package datawave.query.language.builder.jexl;

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

import datawave.query.language.parser.jexl.JexlNode;
import datawave.query.language.parser.jexl.JexlPhraseNode;
import datawave.query.language.parser.jexl.JexlSelectorNode;
import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;
import datawave.query.search.WildcardFieldedTerm;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.EscapedNodes;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.search.TermQuery;

import com.google.common.collect.Sets;

/**
 * Builds a {@link TermQuery} object from a {@link FieldQueryNode} object.
 */
public class FieldQueryNodeBuilder implements QueryBuilder {
    private static final String WHITE_SPACE_ESCAPE_STRING = "~~";
    public static final String SPACE = " ";
    
    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        JexlNode returnNode = null;
        
        if (queryNode instanceof QuotedFieldQueryNode) {
            List<String> phraseWordList = new ArrayList<>();
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
                    phraseWordList.add(currWord);
                }
            }
            
            if (phraseWordList.size() == 1) {
                // if only one term in quotes, just use a SelectorNode
                String firstWord = phraseWordList.get(0);
                if (field == null || field.isEmpty()) {
                    returnNode = new JexlSelectorNode(JexlSelectorNode.Type.EXACT, "", firstWord);
                } else {
                    returnNode = new JexlSelectorNode(JexlSelectorNode.Type.EXACT, field, firstWord);
                }
            } else {
                // if more than one term in quotes, use an AdjNode
                returnNode = new JexlPhraseNode(field, phraseWordList);
            }
        } else if (queryNode instanceof FuzzyQueryNode) {
            throw new UnsupportedOperationException(queryNode.getClass().getName() + " not implemented");
        } else {
            // queryNode instanceof WildcardQueryNode
            // queryNode instanceof QuotedFieldQueryNode
            // queryNode instanceof FieldQueryNode
            
            FieldQueryNode fieldNode = (FieldQueryNode) queryNode;
            boolean hasUnescapedWildcard = WildcardFieldedTerm.hasUnescapedWildcard(fieldNode, Sets.newHashSet(' ', '/'));
            
            String field = fieldNode.getFieldAsString();
            String selector;
            JexlSelectorNode.Type type;
            if (hasUnescapedWildcard) {
                // When we have a regular expression, we want to maintain the original escaped characters in the term
                selector = EscapedNodes.getEscapedTerm(fieldNode, new EscapeQuerySyntaxImpl());
                type = JexlSelectorNode.Type.WILDCARDS;
            } else {
                selector = fieldNode.getTextAsString();
                type = JexlSelectorNode.Type.EXACT;
            }
            
            if (field == null || field.isEmpty()) {
                returnNode = new JexlSelectorNode(type, "", selector);
            } else {
                returnNode = new JexlSelectorNode(type, field, selector);
            }
        }
        
        return returnNode;
    }
    
    private void replaceEscapeCharsWithSpace(ArrayList<String> words) {
        for (int i = 0; i < words.size(); i++) {
            words.set(i, words.get(i).replaceAll(WHITE_SPACE_ESCAPE_STRING, SPACE));
        }
    }
    
}
