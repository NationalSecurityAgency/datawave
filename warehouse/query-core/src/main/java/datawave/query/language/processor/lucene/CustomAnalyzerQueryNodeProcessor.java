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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
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
 * Uses the {@link Analyzer} specified in the the {@link ConfigurationKeys#ANALYZER} attribute of the {@link QueryConfigHandler} to process non-wildcard
 * {@link FieldQueryNode}s for fields listed in <code>tokenizedFields</code>.
 * <p>
 * (Nodes that are {@link WildcardQueryNode}, {@link FuzzyQueryNode} or {@link RegexpQueryNode} or are part of a {@link TermRangeQueryNode} are NOT processed by
 * this processor.)
 * </p>
 * <p>
 * The text of each {@link TextableQueryNode} is processed using the {@link Analyzer} to generate tokens. If the analyzer returns one or more terms that are not
 * identical to the input, the processor generates an {@link OrQueryNode} containing the original query node and a new {@link QuotedFieldQueryNode} or
 * {@link SlopQueryNode} depending on the nature of the original query node and whether <code>useSlopForTokenizedTerms</code> is <code>false</code>.
 * </p>
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

    private QueryNode tokenizeNode(final QueryNode node, final String text, final String field) throws QueryNodeException {
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
        }

        node.setTag(NODE_PROCESSED, Boolean.TRUE); // mark this node as processed, so we don't process it again.

        try (TokenStream buffer = this.analyzer.tokenStream(field, new StringReader(text))) {

            // prepare the source for reading.
            buffer.reset();

            if (!buffer.hasAttribute(CharTermAttribute.class)) {
                return node; // tokenizer can't produce terms, return unmodified query node.
            }

            final CharTermAttribute termAtt = buffer.getAttribute(CharTermAttribute.class);
            final PositionIncrementAttribute posIncrAtt = buffer.hasAttribute(PositionIncrementAttribute.class)
                            ? buffer.getAttribute(PositionIncrementAttribute.class)
                            : null;

            // the variant builder will maintain multiple versions of the tokenized query as we find tokens
            // that have multiple variants in the same position - e.g., stems, roots or lemmas.
            final VariantBuilder b = new VariantBuilder();

            // build the new query strings from the tokenizer output while tracking cases where we've dropped words
            // and will need to adjust the phrase slop for the query as a result.
            int positionCount = 0;

            while (buffer.incrementToken()) {
                String token = termAtt.toString();
                final int positionIncrement = posIncrAtt != null ? posIncrAtt.getPositionIncrement() : 1;
                positionCount += positionIncrementsEnabled ? positionIncrement : 1;
                b.append(token, positionIncrement == 0);
            }

            if (b.hasNoVariants()) {
                return node; // If we didn't produce anything from the tokenizer, return unmodified query node.
            }

            // calculate the amount of slop we need to add based on the original query and the number of positions observed
            int slopNeeded = calculateSlopNeeded(node, text, positionCount);

            // process each of the 'variants' to ensure they are different from the base query, if so, potentially
            // create a new query node and add it to the set of OR clauses. Variants are guaranteed unique, so no
            // need to deduplicate there.
            final String baseQueryText = getEscapedBaseQueryText(node);
            final LinkedList<QueryNode> clauses = new LinkedList<>();
            for (String tokenizedText : b.getVariants()) {
                if (tokenizedText.equalsIgnoreCase(baseQueryText)) {
                    continue; // skip this variant - it adds nothing new over the base query.
                }
                QueryNode newQueryNode = createNewQueryNode(field, tokenizedText, slopNeeded);
                clauses.add(newQueryNode);
            }

            if (clauses.isEmpty()) {
                return node;
            }

            // If we made it here, the tokenizer produced output that was different from the original query node, and
            // we want to build an 'OR' clause that will match either query string.
            clauses.addFirst(possiblyWrapOriginalQueryNode(node));
            return new GroupQueryNode(new OrQueryNode(clauses));
        } catch (IOException e) {
            throw new QueryNodeException(e);
        }
    }

    /**
     * Create a new query node for the specified field and tokenize text, optionally wrapping it in a SlopQueryNode if we've determined that slop is needed
     * (either due to tokens being removed or there being slop on the original query we need to account for.
     *
     * @param field
     *            the field for the query node
     * @param tokenizedText
     *            the text for the query node
     * @param slopNeeded
     *            whether slop is needed.
     * @return a new QuotedFieldQueryNode or possibly a SlopQueryNode containing the new clause. Both of these nodes will be marked as 'PROCESSED'.
     */
    public QueryNode createNewQueryNode(String field, String tokenizedText, int slopNeeded) {
        QueryNode newQueryNode = new QuotedFieldQueryNode(field, new UnescapedCharSequence(tokenizedText), -1, -1);
        newQueryNode.setTag(NODE_PROCESSED, Boolean.TRUE); // don't process this node again.
        if (slopNeeded != Integer.MIN_VALUE) {
            newQueryNode = new SlopQueryNode(newQueryNode, slopNeeded);
            newQueryNode.setTag(NODE_PROCESSED, Boolean.TRUE); // don't process this node again.
        }
        return newQueryNode;
    }

    /**
     * Calculate the amount of slop we need to add to a new query node for tokenized text. This is based on the based on the number of positions observed in the
     * tokenized text and the difference between the slop in the original query minus the original token count.
     *
     * @param node
     *            the original query node from which the tokenized text originated.
     * @param text
     *            the text of the original query.
     * @param positionsObserved
     *            the number of positions observed in the tokenized text.
     * @return the amount of slop we need to add to our new query clauses, Integer.MIN_VALUE if there should be no changes to the original node.
     */
    private int calculateSlopNeeded(QueryNode node, String text, int positionsObserved) {
        int slopNeeded = positionsObserved;

        // Adjust the slop based on the difference between the original
        // slop minus the original token count (based on whitespace)
        int originalSlop = 0;
        if (node.getTag(ORIGINAL_SLOP) != null) {
            originalSlop = (Integer) node.getTag(ORIGINAL_SLOP);
            final int delta = originalSlop - text.split("\\s+").length;
            slopNeeded += delta;
        }

        final boolean originalWasQuoted = QuotedFieldQueryNode.class.isAssignableFrom(node.getClass());
        if ((useSlopForTokenizedTerms && !originalWasQuoted) || originalSlop > 0) {
            return slopNeeded;
        }

        return Integer.MIN_VALUE;
    }

    /**
     * If the original query node was nested in a SlopQueryNode, that fact has been stored in the ORIGINAL_SLOP tag, and we'll need to re-create that slop node.
     * Otherwise, return the original node unchanged.
     *
     * @param node
     *            the node to process.
     * @return the node wrapped in a SlopQueryNode, if the input node originally had slop.
     */
    private static QueryNode possiblyWrapOriginalQueryNode(QueryNode node) {
        final int originalSlop = node.getTag(ORIGINAL_SLOP) != null ? (Integer) node.getTag(ORIGINAL_SLOP) : 0;
        final QueryNode originalQueryNode = originalSlop > 0 ? new SlopQueryNode(node, originalSlop) : node;
        originalQueryNode.setTag(NODE_PROCESSED, Boolean.TRUE);
        return originalQueryNode;
    }

    /**
     * If a query node was something that has text, get the text. If the query node was already unescaped, convert it to it's escaped version. This way it can
     * be compared to other nodes with escapes in place.
     *
     * @param node
     *            the node to extract text from
     * @return the escaped version of the text from the node, null if the node had no text.
     */
    private static String getEscapedBaseQueryText(QueryNode node) {
        if (TextableQueryNode.class.isAssignableFrom(node.getClass())) {
            final CharSequence c = ((TextableQueryNode) node).getText();
            return UnescapedCharSequence.class.isAssignableFrom(c.getClass()) ? toStringEscaped((UnescapedCharSequence) c) : c.toString();
        }
        return null;
    }

    /**
     * Work around a Lucene Bug in UnescapedCharSequence.toStringEscaped()
     *
     * @param unescaped
     *            string value
     * @return unescaped string
     */
    private static String toStringEscaped(UnescapedCharSequence unescaped) {
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

    /**
     * Maintains one or more buffers for tokenized queries. During standard operation, works like a StringBuilder. If the tokenizer encounters a variant (e.g.,
     * zero position offset, same start and end as the previous token) appendVariant will start building a second buffer containing that variant.
     */
    public static class VariantBuilder {
        List<List<String>> variants = new ArrayList<>();

        public VariantBuilder append(String input, boolean appendVariant) {
            return appendVariant ? appendVariant(input) : append(input);
        }

        public VariantBuilder append(String input) {
            if (variants.isEmpty()) {
                variants.add(new ArrayList<>());
            }

            for (List<String> b : variants) {
                b.add(input);
            }

            return this;
        }

        public VariantBuilder appendVariant(String input) {
            if (variants.isEmpty()) {
                append(input);
            } else {

                List<List<String>> newVariants = new ArrayList<>();

                for (List<String> b : variants) {
                    // create a new variant of all the existing strings, replacing the
                    List<String> newVariant = new ArrayList<>(b);
                    newVariant.set(newVariant.size() - 1, input);
                    newVariants.add(newVariant);
                }

                variants.addAll(newVariants);
            }

            return this;
        }

        public boolean hasNoVariants() {
            boolean hasNoVariants = true;
            for (List<String> b : variants) {
                if (!b.isEmpty()) {
                    // at least one of the variant buffers has something.
                    hasNoVariants = false;
                    break;
                }
            }
            return hasNoVariants;
        }

        public Set<String> getVariants() {
            Set<String> result = new TreeSet<>();
            for (List<String> b : variants) {
                result.add(String.join(" ", b));
            }
            return result;
        }
    }
}
