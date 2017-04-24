package nsa.datawave.query.language.processor.lucene;

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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nsa.datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

/**
 * This processor verifies if the attribute {@link ConfigurationKeys#ANALYZER} is defined in the {@link QueryConfigHandler}. If it is and the analyzer is not
 * <code>null</code>, it looks for every {@link FieldQueryNode} that is not {@link WildcardQueryNode}, {@link FuzzyQueryNode} or {@link RegexpQueryNode}
 * contained in the query node tree, then it applies the analyzer to that {@link FieldQueryNode} object if the field name is present in the
 * <code>tokenizedFields</code> set. <br>
 * <br>
 * If the analyzer returns one or more terms that are not identical to the input, an {@link OrQueryNode} containing the original query node and a
 * {@link QuotedFieldQueryNode} containing whitespace delimited tokens is returned. <br>
 * If no term is returned by the analyzer the original query node is returned. <br>
 * If unfieldedTokenized is set to true, nodes
 * 
 * @see Analyzer
 * @see TokenStream
 */
public class CustomAnalyzerQueryNodeProcessor extends QueryNodeProcessorImpl {
    
    private final static Logger logger = Logger.getLogger(CustomAnalyzerQueryNodeProcessor.class);
    
    private Analyzer analyzer;
    
    private boolean positionIncrementsEnabled;
    
    private boolean unfieldedTokenized = false;
    private Set<String> tokenizedFields = new HashSet<>();
    private Set<String> skipTokenizeUnfieldedFields = new HashSet<>();
    private boolean tokensAsPhrase = false;
    
    public CustomAnalyzerQueryNodeProcessor() {}
    
    @Override
    public QueryNode process(QueryNode queryTree) throws QueryNodeException {
        
        if (getQueryConfigHandler().has(ConfigurationKeys.ANALYZER)) {
            this.analyzer = getQueryConfigHandler().get(ConfigurationKeys.ANALYZER);
        }
        
        this.positionIncrementsEnabled = false;
        
        if (getQueryConfigHandler().has(ConfigurationKeys.ENABLE_POSITION_INCREMENTS)) {
            
            if (getQueryConfigHandler().get(ConfigurationKeys.ENABLE_POSITION_INCREMENTS)) {
                this.positionIncrementsEnabled = true;
            }
        }
        
        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.TOKENIZED_FIELDS)) {
            tokenizedFields.addAll(getQueryConfigHandler().get(LuceneToJexlQueryParser.TOKENIZED_FIELDS));
        }
        
        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.SKIP_TOKENIZE_UNFIELDED_FIELDS)) {
            skipTokenizeUnfieldedFields.clear();
            skipTokenizeUnfieldedFields.addAll(getQueryConfigHandler().get(LuceneToJexlQueryParser.SKIP_TOKENIZE_UNFIELDED_FIELDS));
        }
        
        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.TOKENIZE_UNFIELDED_QUERIES)) {
            this.unfieldedTokenized = getQueryConfigHandler().get(LuceneToJexlQueryParser.TOKENIZE_UNFIELDED_QUERIES);
        }
        
        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.TOKENS_AS_PHRASE)) {
            this.tokensAsPhrase = getQueryConfigHandler().get(LuceneToJexlQueryParser.TOKENS_AS_PHRASE);
        }
        
        QueryNode processedQueryTree = super.process(queryTree);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Analyzer: " + analyzer);
            logger.debug("Position Increments Enabled: " + positionIncrementsEnabled);
            logger.debug("TokenizedFields: " + Arrays.toString(tokenizedFields.toArray()));
            logger.debug("SkipTokenizeUnfieldedFields: " + Arrays.toString(skipTokenizeUnfieldedFields.toArray()));
            logger.debug("Tokenize Unfielded Queries: " + this.unfieldedTokenized);
            logger.debug("Tokens As Phrase: " + this.tokensAsPhrase);
            logger.debug("Original QueryTree: " + queryTree.toString());
            logger.debug("Processed QueryTree: " + queryTree.toString());
        }
        
        return processedQueryTree;
    }
    
    @Override
    protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
        
        if (logger.isDebugEnabled()) {
            logger.debug("Incoming query node: " + node.toString());
        }
        
        if (node instanceof TextableQueryNode && !(node instanceof WildcardQueryNode) && !(node instanceof RegexpQueryNode)
                        && !(node instanceof FuzzyQueryNode)) {
            
            if (node.getParent() != null && node.getParent() instanceof TermRangeQueryNode) {
                // Ignore children of TermReangeQueryNodes (for now)
                return node;
            }
            
            FieldQueryNode fieldNode = ((FieldQueryNode) node);
            String text = fieldNode.getTextAsString();
            String field = fieldNode.getFieldAsString();
            
            // treat these fields as unfielded and skip tokenization if enabled.
            if (skipTokenizeUnfieldedFields.contains(field)) {
                fieldNode.setField("");
                
                if (logger.isDebugEnabled()) {
                    logger.debug("Skipping tokenization of unfielded query node: " + fieldNode);
                }
                
                return fieldNode;
            }
            
            if ((tokenizedFields.contains(field) || (unfieldedTokenized && field.isEmpty()))) {
                node = tokenizeNode(node, text, field);
            }
            
            if (logger.isDebugEnabled()) {
                logger.debug("Post-processed query node: " + node.toString());
            }
        }
        
        return node;
    }
    
    protected QueryNode tokenizeNode(QueryNode node, String text, String field) throws QueryNodeException {
        CachingTokenFilter buffer = null;
        
        if (analyzer == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping tokenization of node: '" + node + "'; no analyzer is set");
            }
            return node;
        }
        
        try {
            // take a pass over the tokens and buffer them in the caching token filter.
            TokenStream source = this.analyzer.tokenStream(field, new StringReader(text));
            source.reset();
            
            buffer = new CachingTokenFilter(source);
            
            PositionIncrementAttribute posIncrAtt = null;
            int numTokens = 0;
            
            if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
                posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
            }
            
            while (buffer.incrementToken()) {
                numTokens++;
            }
            
            // rewind the buffer stream
            buffer.reset();
            // close original stream - all tokens buffered
            source.close();
            
            if (!buffer.hasAttribute(CharTermAttribute.class) || numTokens == 0) {
                // no terms found, return unmodified node.
                return node;
            }
            
            CharTermAttribute termAtt = buffer.getAttribute(CharTermAttribute.class);
            
            StringBuilder b = new StringBuilder();
            int slopRange = 0;
            
            String term = null;
            while (buffer.incrementToken()) {
                term = termAtt.toString();
                if (text.toLowerCase().equals(term)) {
                    // token is identical to term, return original node.
                    return node;
                }
                
                b.append(term).append(" ");
                if (posIncrAtt != null && this.positionIncrementsEnabled) {
                    slopRange += posIncrAtt.getPositionIncrement();
                } else {
                    slopRange++;
                }
            }
            
            b.setLength(b.length() - 1); // trim trailing whitespace
            
            if (b.length() > 0) {
                final String tokenizedText = b.toString();
                QueryNode n = new QuotedFieldQueryNode(field, new UnescapedCharSequence(tokenizedText), -1, -1);
                
                if (!tokensAsPhrase) {
                    n = new SlopQueryNode(n, slopRange);
                }
                
                // skip adding a OR with duplicate QuotesField nodes that contain the
                // same txt, possibly replacing a 'phrase' node with a 'within' node for
                // consistent behavior when tokensAsPhrase is false. We compare the 'escaped'
                // string of the orignal query so we don't mistreate things like spaces.
                if (QuotedFieldQueryNode.class.isAssignableFrom(node.getClass())) {
                    CharSequence c = ((QuotedFieldQueryNode) node).getText();
                    if (UnescapedCharSequence.class.isAssignableFrom(c.getClass())) {
                        c = toStringEscaped((UnescapedCharSequence) c);
                    }
                    if (tokenizedText.contentEquals(c)) {
                        return n;
                    }
                }
                
                final List<QueryNode> clauses = new ArrayList<>();
                clauses.add(node);
                clauses.add(n);
                
                node = new GroupQueryNode(new OrQueryNode(clauses));
            }
        } catch (IOException e) {
            throw new QueryNodeException(e);
        } finally {
            if (buffer != null) {
                try {
                    buffer.close();
                } catch (IOException ex) {
                    logger.warn("Exception closing caching token filter: ", ex);
                }
            }
        }
        
        return node;
    }
    
    /** Work around a Lucene Bug in UnescapedCharSequence.toStringEscaped() */
    private final String toStringEscaped(UnescapedCharSequence unescaped) {
        // non efficient implementation
        final StringBuilder result = new StringBuilder();
        final int len = unescaped.length();
        for (int i = 0; i < len; i++) {
            if (unescaped.charAt(i) == '\\') {
                result.append('\\');
            } else if (unescaped.wasEscaped(i))
                result.append('\\');
            
            result.append(unescaped.charAt(i));
        }
        return result.toString();
    }
    
    @Override
    protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
        
        return node;
        
    }
    
    @Override
    protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {
        
        return children;
        
    }
    
}
