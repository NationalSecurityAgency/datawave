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
package datawave.query.language.processor.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
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
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import datawave.query.Constants;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

/**
 * Adds analyzer-generated alternatives to configured query fields, keeping the original query as the first OR clause.
 * <p>
 * An {@link Analyzer} turns query text into tokens (word strings). It can change a word's spelling or split one input word into several tokens, such as
 * {@code wi-fi} into {@code wi} and {@code fi}. This processor uses the analyzer from {@link ConfigurationKeys#ANALYZER} for {@link FieldQueryNode}s whose
 * fields are listed in {@code tokenizedFields}. Wildcards, fuzzy terms, regular expressions, and children of range queries are left unchanged.
 * </p>
 * <p>
 * A position is one word slot in the analyzer's output. A positive {@link PositionIncrementAttribute} starts a new position; zero means another form of the
 * word at the current position. A token's {@link TypeAttribute} labels how that form was produced. For example, a filter may label dictionary forms
 * {@code LEMMA} and shortened forms {@code STEM}. Here, "modality" means that type label; no particular labels are required.
 * </p>
 * <p>
 * Expansion is per modality. A track is a phrase under construction, with one token per position. The primary track uses the first token at each position. Each
 * type also gets a track, using its own first form when available and the primary form otherwise. Additional forms of one type use separate tracks, called
 * spill tracks: second forms share a track across positions, third forms share another, and so on. Where a spill form is missing, its track uses that type's
 * first form, or the primary form if the type is absent.
 * </p>
 * <p>
 * Correctness assumes token filters emit same-position alternatives with distinct, stable type strings. The processor intentionally stops generating every
 * combination of forms across positions. For example, it no longer independently generates a phrase using a lemma at one position and a stem at another. The
 * original and primary phrases remain, and missing-type fallbacks can still contain forms of other types. Counting primary among the type tracks, total OR
 * clauses are bounded by type tracks plus spill tracks plus one for the original, regardless of phrase length for fixed types and form counts. Duplicate
 * phrases and alternatives equal to the original text are skipped.
 * </p>
 * <p>
 * Each distinct alternative becomes a {@link QuotedFieldQueryNode}, optionally wrapped in a {@link SlopQueryNode}. Slop controls the allowed distance between
 * phrase terms. Existing slop adjustments apply to all tracks: for example, splitting {@code "portable wi-fi"~3} produces {@code "portable wi fi"~4}. An
 * unquoted input that expands to several words also gets slop when {@code useSlopForTokenizedTerms} is true; otherwise its alternative is an exact phrase.
 * </p>
 * <p>
 * An empty field means the query is unfielded, and it is analyzed when {@code unfieldedTokenized} is true. Fields in {@code skipTokenizeUnfieldedFields} are
 * made unfielded without analyzing their text.
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

    /** Include analyzer position gaps in the count used to adjust slop. */
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

    /** Allow distance between words when an unquoted input expands to several tokens. */
    private boolean useSlopForTokenizedTerms = true;

    /**
     * Read the analyzer settings from {@link QueryConfigHandler} and process the supplied query tree.
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
            } else if (PrefixWildcardQueryNode.class.isAssignableFrom(nodeClazz)) {
                return node;
            } else if (FuzzyQueryNode.class.isAssignableFrom(nodeClazz)) {
                return node;
            } else if (RegexpQueryNode.class.isAssignableFrom(nodeClazz)) {
                return node;
            } else if (node.getParent() != null && TermRangeQueryNode.class.isAssignableFrom(node.getParent().getClass())) {
                // Ignore children of TermReangeQueryNodes (for now)
                return node;
            }

            final TextableQueryNode textableNode = (TextableQueryNode) node;
            final String text = textableNode.getText().toString();

            FieldQueryNode fieldNode;
            String field = Constants.EMPTY_STRING;

            if (FieldQueryNode.class.isAssignableFrom(nodeClazz)) {
                fieldNode = (FieldQueryNode) node;
                field = fieldNode.getFieldAsString();

                // treat these fields as un-fielded and skip tokenization if enabled.
                if (skipTokenizeUnfieldedFields.contains(field.toUpperCase())) {
                    fieldNode.setField(Constants.EMPTY_STRING);

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

    /**
     * Analyze one query node and add its distinct phrase alternatives. Position increments determine word slots for the builder; the separate
     * {@code positionCount} keeps the existing slop calculation, including its behavior when position increments are disabled.
     *
     * @param node
     *            the original query node
     * @param text
     *            the original query text
     * @param field
     *            the query field, or an empty string for an unfielded query
     * @return the original node if no distinct alternative is found, otherwise an OR group with the original first
     * @throws QueryNodeException
     *             if reading the analyzer output fails
     */
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
            final TypeAttribute typeAtt = buffer.hasAttribute(TypeAttribute.class) ? buffer.getAttribute(TypeAttribute.class) : null;

            // Keep each type's forms together across words instead of generating every combination.
            final ModalityVariantBuilder b = new ModalityVariantBuilder();

            // build the new query strings from the tokenizer output while tracking cases where we've dropped words
            // and will need to adjust the phrase slop for the query as a result.
            int positionCount = 0;

            while (buffer.incrementToken()) {
                String token = termAtt.toString();
                final int positionIncrement = posIncrAtt != null ? posIncrAtt.getPositionIncrement() : 1;
                positionCount += positionIncrementsEnabled ? positionIncrement : 1;
                b.append(token, typeAtt != null ? typeAtt.type() : null, positionIncrement == 0);
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
     * Create a phrase alternative and mark it as processed so it will not be analyzed again. Wrap it in a {@link SlopQueryNode} when an allowed distance
     * between terms is supplied.
     *
     * @param field
     *            the field for the query node
     * @param tokenizedText
     *            the analyzed tokens joined into a phrase
     * @param slopNeeded
     *            the allowed distance, or {@link Integer#MIN_VALUE} to leave the phrase without a slop wrapper
     * @return a processed phrase node, optionally inside a processed slop node
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
     * Determine the slop for an analyzed phrase. If the input had slop, adjust it by the difference between the analyzer's position count and the number of
     * whitespace-separated input words. This accounts for words removed or split by the analyzer.
     *
     * @param node
     *            the original query node from which the tokenized text originated.
     * @param text
     *            the text of the original query.
     * @param positionsObserved
     *            the number of positions observed in the tokenized text.
     * @return the slop for generated phrases, or {@link Integer#MIN_VALUE} when they should have no slop wrapper
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
     * Get the original text with its escape characters restored so it can be compared with analyzer-generated alternatives.
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
     * Restore escape characters that Lucene's {@link UnescapedCharSequence#toStringEscaped()} does not preserve.
     *
     * @param unescaped
     *            string value
     * @return the text with its escape characters restored
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
     * Builds a small set of alternative phrases from the analyzer's word strings.
     * <p>
     * A position is a word slot, and a track is a list with one word per slot. The primary track uses the first word at each position. A type's base track uses
     * its first form there, falling back to primary when that type is absent. If a type supplies a second distinct form, that form goes into a second track;
     * third forms go into a third track. Those extra tracks use the base track's word wherever their own form is missing.
     * </p>
     * <p>
     * The same numbered track is reused across positions. For example, if one type emits {@code a, x} at the first position and {@code b, y} at the next, the
     * phrases are {@code a b} and {@code x y}; the builder does not also create {@code a y} and {@code x b}. Sequential tokens, such as the two words produced
     * by splitting {@code wi-fi}, start separate positions and remain in order in every phrase.
     * </p>
     * <p>
     * A new type's track copies the preceding primary words. A new extra track copies its type's preceding base words. Types are arbitrary strings, and track
     * numbers are stored separately from them, so a real type name ending in {@code #2} cannot be mistaken for an extra track.
     * </p>
     */
    public static class ModalityVariantBuilder {
        /** First forms at completed positions. */
        private final List<String> primary = new ArrayList<>();
        /** Type to phrase lists: index zero holds first forms, index one holds second forms, and so on. */
        private final Map<String,List<List<String>>> tracks = new LinkedHashMap<>();
        /** Distinct forms at the current position, grouped by type and kept in emission order. */
        private final Map<String,List<String>> positionForms = new LinkedHashMap<>();
        /** First form at the current position, or null when no position is pending. */
        private String positionPrimary;

        /**
         * Create a builder with no tokens.
         */
        public ModalityVariantBuilder() {}

        /**
         * Copy the completed words and current position so a variant read can finish a temporary copy without changing the source builder.
         */
        private ModalityVariantBuilder(ModalityVariantBuilder source) {
            primary.addAll(source.primary);
            for (Map.Entry<String,List<List<String>>> entry : source.tracks.entrySet()) {
                List<List<String>> typeTracks = new ArrayList<>();
                for (List<String> track : entry.getValue()) {
                    typeTracks.add(new ArrayList<>(track));
                }
                tracks.put(entry.getKey(), typeTracks);
            }
            for (Map.Entry<String,List<String>> entry : source.positionForms.entrySet()) {
                positionForms.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            positionPrimary = source.positionPrimary;
        }

        /**
         * Add a token to the current word position or start the next position. The first token always starts a position, even if {@code samePosition} is true.
         * Repeated identical forms of the same type at one position are ignored.
         *
         * @param input
         *            the token text
         * @param type
         *            the analyzer's type label; {@code null} groups tokens whose stream has no type attribute
         * @param samePosition
         *            true for another form at the current position (position increment zero), false for a new position
         * @return this builder
         */
        public ModalityVariantBuilder append(String input, String type, boolean samePosition) {
            if (!samePosition) {
                finishPosition();
            }
            if (positionPrimary == null) {
                positionPrimary = input;
            }
            List<String> forms = positionForms.computeIfAbsent(type, key -> new ArrayList<>());
            if (!forms.contains(input)) {
                forms.add(input);
            }
            return this;
        }

        /**
         * Finish the current word position. Create any newly needed tracks from preceding words, then add exactly one word to every track using its own form or
         * fallback.
         */
        private void finishPosition() {
            if (positionPrimary == null) {
                return;
            }

            // New types copy preceding primary words; extra forms copy preceding words from their type's base track.
            for (Map.Entry<String,List<String>> entry : positionForms.entrySet()) {
                List<List<String>> typeTracks = tracks.computeIfAbsent(entry.getKey(), key -> new ArrayList<>());
                if (typeTracks.isEmpty()) {
                    typeTracks.add(new ArrayList<>(primary));
                }
                while (typeTracks.size() < entry.getValue().size()) {
                    typeTracks.add(new ArrayList<>(typeTracks.get(0)));
                }
            }

            for (Map.Entry<String,List<List<String>>> entry : tracks.entrySet()) {
                List<String> forms = positionForms.get(entry.getKey());
                String base = forms != null ? forms.get(0) : positionPrimary;
                List<List<String>> typeTracks = entry.getValue();
                for (int i = 0; i < typeTracks.size(); i++) {
                    typeTracks.get(i).add(forms != null && i < forms.size() ? forms.get(i) : base);
                }
            }
            primary.add(positionPrimary);
            positionForms.clear();
            positionPrimary = null;
        }

        /**
         * Check whether any tokens have been added, including tokens at the current position.
         *
         * @return true if the builder has no tokens
         */
        public boolean hasNoVariants() {
            return primary.isEmpty() && positionPrimary == null;
        }

        /**
         * Read the distinct phrases built so far, joining their tokens with spaces. Reading does not close the current position or change previously returned
         * sets, so more forms can still be added at that position. The original query is added separately by the processor.
         *
         * @return an independent {@link TreeSet} of phrase strings in lexical order, or an empty set if no tokens were added
         */
        public Set<String> getVariants() {
            if (positionPrimary == null) {
                return collectVariants();
            }
            ModalityVariantBuilder snapshot = new ModalityVariantBuilder(this);
            snapshot.finishPosition();
            return snapshot.collectVariants();
        }

        /**
         * Join the completed tracks into sorted, distinct phrase strings.
         */
        private Set<String> collectVariants() {
            Set<String> result = new TreeSet<>();
            if (!primary.isEmpty()) {
                result.add(String.join(Constants.SPACE, primary));
            }
            for (List<List<String>> typeTracks : tracks.values()) {
                for (List<String> track : typeTracks) {
                    result.add(String.join(Constants.SPACE, track));
                }
            }
            return result;
        }
    }

    /**
     * Legacy builder that generates all combinations of word alternatives. Adding another form at the current position copies every existing phrase and
     * replaces its last word, so the number of phrases can grow exponentially with phrase length. Kept for callers of earlier releases; this processor uses
     * {@link ModalityVariantBuilder}.
     *
     * @deprecated Use {@link ModalityVariantBuilder} to keep one phrase per type and numbered extra form.
     */
    @Deprecated
    public static class VariantBuilder {
        List<List<String>> variants = new ArrayList<>();

        /**
         * Add a token at the next position or as another form of the current word.
         *
         * @param input
         *            the token text
         * @param appendVariant
         *            true to add another form at the current position, false to start the next position
         * @return this builder
         */
        public VariantBuilder append(String input, boolean appendVariant) {
            return appendVariant ? appendVariant(input) : append(input);
        }

        /**
         * Add the same token to the end of every phrase.
         *
         * @param input
         *            the token text
         * @return this builder
         */
        public VariantBuilder append(String input) {
            if (variants.isEmpty()) {
                variants.add(new ArrayList<>());
            }

            for (List<String> b : variants) {
                b.add(input);
            }

            return this;
        }

        /**
         * Copy each phrase and replace its last token with this form, preserving the existing phrases. If there are no phrases yet, start one with this token.
         *
         * @param input
         *            another form for the current position
         * @return this builder
         */
        public VariantBuilder appendVariant(String input) {
            if (variants.isEmpty()) {
                append(input);
            } else {

                List<List<String>> newVariants = new ArrayList<>();

                for (List<String> b : variants) {
                    // Copy the phrase and replace its last word.
                    List<String> newVariant = new ArrayList<>(b);
                    newVariant.set(newVariant.size() - 1, input);
                    newVariants.add(newVariant);
                }

                variants.addAll(newVariants);
            }

            return this;
        }

        /**
         * Check whether any tokens have been added.
         *
         * @return true if all phrase lists are empty
         */
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

        /**
         * Read the distinct phrase strings without changing the builder.
         *
         * @return an independent {@link TreeSet} of phrase strings in lexical order
         */
        public Set<String> getVariants() {
            Set<String> result = new TreeSet<>();
            for (List<String> b : variants) {
                result.add(String.join(Constants.SPACE, b));
            }
            return result;
        }
    }
}
