package datawave.query.language.processor.lucene;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

/**
 * <p>
 * Applies tokenization to {@link TextableQueryNode} objects using a configured Lucene {@link Analyzer}.
 * </p>
 *
 * Uses the {@link Analyzer} specified in the the {@link ConfigurationKeys#ANALYZER} attribute of the {@link QueryConfigHandler} to process non-wildcard
 * {@link FieldQueryNode}s for fields listed in <code>tokenizedFields</code>.
 *
 * (Nodes that are {@link WildcardQueryNode}, {@link FuzzyQueryNode} or {@link RegexpQueryNode} or are part of a {@link TermRangeQueryNode} are NOT processed by
 * this processor.)
 *
 * The text of each {@link TextableQueryNode} is processed using the {@link Analyzer} to generate tokens. If the analyzer returns one or more terms that are not
 * identical to the input, the processor generates an {@link OrQueryNode} containing the original query node and a new {@link QuotedFieldQueryNode} or
 * {@link SlopQueryNode} depending on the nature of the original query node and whether <code>useSlopForTokenizedTerms</code> is <code>false</code>.
 * <p>
 * There are three primary cases where tokenization will be applied to input query terms - single terms (e.g: wi-fi), phrases (e.g: "portable wi-fi"), and
 * phrases with slop (e.g: "portable wi-fi"~3). In the case of single term input, tokenization will produce a phrase with slop equals to the number of positions
 * in the original query if <code>useSlopForTokenizedTerms</code> is set to <code>true</code>, otherwise a phrase without slop will be produced. In the case of
 * phrase input, a new phrase query will be generated with the new tokens. In t he case of a phrase with slop, a new phrase with slop will be generated and an
 * attempt will be made to adjust the slop based on the number of additional tokens generated. For exa mple, in the case of the slop query above, the new query
 * will be "portable wi fi"~4 because an additional token was generated based on the split of 'wi' and 'fi' into two separate tokens.
 * </p>
 * <p>
 * FieldQueryNodes with empty fields are considered 'unfielded' and will be tokenized if <code>unfieldedTokenized</code> is <code>true</code>. The
 * <code>skipTokenizeUnfieldedFields</code> can be used in this case to indicate that a node should be treated as un-fielded but not tokenized. When this
 * processor encounters such a field in a node, it will not tokenize the text of that node and will set that node's field to an empty string so that downstream
 * processors will treat the node as if it is un-fielded.
 * </p>
 *
 * @see Analyzer
 * @see TokenStream
 */
public class CustomAnalyzerQueryNodeProcessor extends QueryNodeProcessorImpl {

    private static final Logger logger = Logger.getLogger(CustomAnalyzerQueryNodeProcessor.class);

    /**
     * A tag added to query nodes that indicates that they have been processed by the {@link CustomAnalyzerQueryNodeProcessor} and should not be processed if
     * visited a second time.
     */
    private static final String NODE_PROCESSED = CustomAnalyzerQueryNodeProcessor.class.getSimpleName() + "_PROCESSED";

    /** Captures the original slop for a {@link SlopQueryNode} in cases where we tokenize the underlying {@link QuotedFieldQueryNode} */
    private static final String ORIGINAL_SLOP = CustomAnalyzerQueryNodeProcessor.class.getSimpleName() + "_ORIGINAL_SLOP";

    /** use this Analyzer to tokenize query nodes */
    private Analyzer analyzer;

    /** track positon increments when tokenizing, potentially used for handing stop words. */
    private boolean positionIncrementsEnabled;

    /** determines whether nodes with empty fields should be tokenized */
    private boolean unfieldedTokenized = false;

    /** the list of fields to tokenize */
    private final Set<String> tokenizedFields = new HashSet<>();

    /**
     * special fields, don't tokenize these if <code>unfieldedTokenized</code> is true, and remove them from the query node so downstream processors treat the
     * node as un-fielded.
     */
    private final Set<String> skipTokenizeUnfieldedFields = new HashSet<>();

    /** treat tokenized test as a phrase (as opposed to a phrase-with-slop) */
    private boolean useSlopForTokenizedTerms = true;

    /**
     * establish configuration from <code>QueryConfigHandler</code> and process the speicfied tree
     *
     * @param queryTree
     *            the query tree to process
     * @return the processed tree.
     * @throws QueryNodeException
     *             if there's a problem processing the tree
     */
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
            getQueryConfigHandler().get(LuceneToJexlQueryParser.TOKENIZED_FIELDS).forEach(s -> tokenizedFields.add(s.toUpperCase()));
        }

        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.SKIP_TOKENIZE_UNFIELDED_FIELDS)) {
            skipTokenizeUnfieldedFields.clear();
            getQueryConfigHandler().get(LuceneToJexlQueryParser.SKIP_TOKENIZE_UNFIELDED_FIELDS).forEach(s -> skipTokenizeUnfieldedFields.add(s.toUpperCase()));
        }

        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.TOKENIZE_UNFIELDED_QUERIES)) {
            this.unfieldedTokenized = getQueryConfigHandler().get(LuceneToJexlQueryParser.TOKENIZE_UNFIELDED_QUERIES);
        }

        if (getQueryConfigHandler().has(LuceneToJexlQueryParser.USE_SLOP_FOR_TOKENIZED_TERMS)) {
            this.useSlopForTokenizedTerms = getQueryConfigHandler().get(LuceneToJexlQueryParser.USE_SLOP_FOR_TOKENIZED_TERMS);
        }

        QueryNode processedQueryTree = super.process(queryTree);

        if (logger.isDebugEnabled()) {
            logger.debug("Analyzer: " + analyzer);
            logger.debug("Position Increments Enabled: " + positionIncrementsEnabled);
            logger.debug("TokenizedFields: " + Arrays.toString(tokenizedFields.toArray()));
            logger.debug("SkipTokenizeUnfieldedFields: " + Arrays.toString(skipTokenizeUnfieldedFields.toArray()));
            logger.debug("Tokenize Unfielded Queries: " + this.unfieldedTokenized);
            logger.debug("Use Slop for Tokenized Terms: " + this.useSlopForTokenizedTerms);
            logger.debug("Original QueryTree: " + queryTree);
            logger.debug("Processed QueryTree: " + queryTree);
        }

        return processedQueryTree;
    }

    @Override
    protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

        if (logger.isDebugEnabled()) {
            logger.debug("Incoming query node: " + node);
        }

        final Class<?> nodeClazz = node.getClass();

        if (SlopQueryNode.class.isAssignableFrom(nodeClazz)) {
            /*
             * SlopQueryNodes typically contain a QuotedFieldQueryNode, so simply call the preProcessNode method on that child immediately, so that preserve the
             * slop as an attribute and return the result. This also allows us to replace the slop node with the OR node produced in tokenize node.
             */
            SlopQueryNode slopNode = (SlopQueryNode) node;
            QueryNode childNode = slopNode.getChild();
            childNode.setTag(ORIGINAL_SLOP, slopNode.getValue());
            QueryNode newChildNode = preProcessNode(childNode);
            if (childNode != newChildNode) {
                return newChildNode;
            }
            return slopNode;
        } else if (TextableQueryNode.class.isAssignableFrom(nodeClazz)) {

            if (WildcardQueryNode.class.isAssignableFrom(nodeClazz)) {
                return node;
            } else if (FuzzyQueryNode.class.isAssignableFrom(nodeClazz)) {
                return node;
            } else if (node.getParent() != null && TermRangeQueryNode.class.isAssignableFrom(node.getParent().getClass())) {
                // Ignore children of TermReangeQueryNodes (for now)
                return node;
            }

            final TextableQueryNode textableNode = (TextableQueryNode) node;
            final String text = textableNode.getText().toString();

            FieldQueryNode fieldNode;
            String field = "";

            if (FieldQueryNode.class.isAssignableFrom(nodeClazz)) {
                fieldNode = (FieldQueryNode) node;
                field = fieldNode.getFieldAsString();

                // treat these fields as un-fielded and skip tokenization if enabled.
                if (skipTokenizeUnfieldedFields.contains(field.toUpperCase())) {
                    fieldNode.setField("");

                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping tokenization of un-fielded query node: " + fieldNode);
                    }

                    return node;
                }
            }

            if ((tokenizedFields.contains(field.toUpperCase()) || (unfieldedTokenized && field.isEmpty()))) {
                node = tokenizeNode(node, text, field);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Post-processed query node: " + node);
            }
        }

        return node;
    }

    @Override
    protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
        return node; /* no-op */
    }

    @Override
    protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {
        return children; /* no-op */
    }

    private QueryNode tokenizeNode(QueryNode node, final String text, final String field) throws QueryNodeException {
        CachingTokenFilter buffer = null;

        if (analyzer == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping tokenization of node: '" + node + "'; no analyzer is set");
            }
            return node;
        }

        // Skip nodes we've processed already.
        final Object processed = node.getTag(NODE_PROCESSED);
        if (processed != null && processed.equals(Boolean.TRUE)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping processed query node: " + node.toString());
            }

            return node;
        } else {
            // mark the original node processed.
            node.setTag(NODE_PROCESSED, Boolean.TRUE);
        }

        try {
            // Take a pass over the tokens and buffer them in the caching token filter.
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

            final CharTermAttribute termAtt = buffer.getAttribute(CharTermAttribute.class);

            StringBuilder b = new StringBuilder();
            int slopRange = 0;

            String term;
            while (buffer.incrementToken()) {
                term = termAtt.toString();
                b.append(term).append(" ");

                // increment the slop range for the tokenized text based on the
                // positionIncrement attribute if available, otherwise one position
                // per token.
                if (posIncrAtt != null && this.positionIncrementsEnabled) {
                    slopRange += posIncrAtt.getPositionIncrement();
                } else {
                    slopRange++;
                }
            }

            b.setLength(b.length() - 1); // trim trailing whitespace

            if (b.length() > 0) {
                final String tokenizedText = b.toString();

                // Check to see that the tokenizer produced output that was different from the original query node.
                // If so avoid creating an OR clause. We compare the 'escaped' string of the original query so that we
                // do not mistreat things like spaces.
                if (TextableQueryNode.class.isAssignableFrom(node.getClass())) {
                    final CharSequence c = ((TextableQueryNode) node).getText();
                    final String cmp = UnescapedCharSequence.class.isAssignableFrom(c.getClass()) ? toStringEscaped((UnescapedCharSequence) c) : c.toString();
                    if (tokenizedText.equalsIgnoreCase(cmp)) {
                        return node;
                    }
                }

                QueryNode n = new QuotedFieldQueryNode(field, new UnescapedCharSequence(tokenizedText), -1, -1);
                // mark the derived node processed so we don't process it again later.
                n.setTag(NODE_PROCESSED, Boolean.TRUE);

                // Adjust the slop based on the difference between the original
                // slop minus the original token count (based on whitespace)
                int originalSlop = 0;
                if (node.getTag(ORIGINAL_SLOP) != null) {
                    originalSlop = (Integer) node.getTag(ORIGINAL_SLOP);
                    final int delta = originalSlop - text.split("\\s+").length;
                    slopRange += delta;
                }

                // Only add slop if the original had slop, or the original was not a phrase and slop is enabled.
                // Using slop for non-quoted terms is a workaround until the phrase function will accept multiple
                // terms in the same position as a valid match.
                boolean originalWasQuoted = QuotedFieldQueryNode.class.isAssignableFrom(node.getClass());
                if ((useSlopForTokenizedTerms && !originalWasQuoted) || originalSlop > 0) {
                    n = new SlopQueryNode(n, slopRange);
                }

                // The tokenizer produced output that was different from the original query node, wrap the original
                // node and the tokenizer produced node in a OR query. To do this properly, we need to wrap the
                // original node in a slop query node if it was originally in a slop query node.
                if (originalSlop > 0) {
                    // restore the original slop wrapper to the base node if it was present originally.
                    node = new SlopQueryNode(node, originalSlop);
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

    /**
     * Work around a Lucene Bug in UnescapedCharSequence.toStringEscaped()
     *
     * @param unescaped
     *            string value
     * @return unescaped string
     */
    private String toStringEscaped(UnescapedCharSequence unescaped) {
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
}
