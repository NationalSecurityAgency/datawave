package datawave.query.language.processor.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.parser.lucene.QueryConfigHandler;
import datawave.query.language.processor.lucene.CustomAnalyzerQueryNodeProcessor.ModalityVariantBuilder;

/**
 * Checks phrase alternatives using explicit token data rather than a real stemming or dictionary lookup library.
 * <p>
 * Each {@link TokenForm} supplies a word string, a position increment, and a type label. A positive increment starts the next word position; zero supplies
 * another form at the current position. The primary phrase uses the first form at each position. A type's phrase uses its own forms, with primary forms where
 * that type is missing. Extra forms of the same type share numbered additional phrases, also called spill tracks.
 * </p>
 * <p>
 * Short words such as {@code p1}, {@code l1}, and {@code s1} represent primary, lemma, and stem forms at position one. Their spelling is test data, not the
 * result of language-specific word processing. {@link SyntheticAnalyzer} emits exactly the supplied forms, so the tests control their order and labels
 * independently of the original query text.
 * </p>
 * <p>
 * Builder assertions check generated strings alone. Parser assertions check the final JEXL query expression, including the original query as the first OR
 * clause. Thus "three variants" means two generated strings plus a distinct original. Exact output assertions also check whether a single word, an exact
 * phrase, or a phrase with an allowed distance is emitted.
 * </p>
 */
class CustomAnalyzerQueryNodeProcessorTest {
    private static final String TOKENIZED_FIELD = "TOKFIELD";
    private static final String TYPE_LEMMA = "LEMMA";
    private static final String TYPE_STEM = "STEM";
    private static final String TYPE_SURFACE = "surface";
    private static final String TYPE_COMPONENT = "component";
    private static final String TYPE_GENERIC = "type";

    /**
     * Emit LEMMA first and STEM second at every position. Primary then equals LEMMA, leaving two generated phrases after deduplication and three final clauses
     * with the original. Testing 1, 4, and 12 positions catches the old growth to every possible combination of word forms.
     *
     * @param positions
     *            the number of word positions in the phrase
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4, 12})
    void twoModalitiesStayBounded(int positions) throws Exception {
        List<TokenForm> tokens = new ArrayList<>();
        for (int i = 0; i < positions; i++) {
            tokens.add(token("lemma" + i, 1, TYPE_LEMMA));
            tokens.add(token("stem" + i, 0, TYPE_STEM));
        }
        String original = phrase("original", positions);
        String lemma = phrase("lemma", positions);
        String stem = phrase("stem", positions);
        assertEquals(variants(lemma, stem), build(tokens).getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(tokens)) {
            // The primary and LEMMA tracks collapse: two generated variants plus the original, at every length.
            assertEquals("(" + clause(original) + " || " + clause(lemma) + " || " + clause(stem) + ")", parse(analyzer, "TOKFIELD:\"" + original + "\""));
        }
    }

    /**
     * Emit a surface form first, then separate lemma and stem forms at every position. The three different phrase strings remain constant in number as the
     * input grows. Primary and the surface type's phrase are identical and must count only once in the returned set.
     *
     * @param positions
     *            the number of word positions in the phrase
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4, 12})
    void distinctPrimaryAddsOnlyOneTrack(int positions) {
        List<TokenForm> tokens = new ArrayList<>();
        for (int i = 0; i < positions; i++) {
            tokens.add(token("surface" + i, 1, TYPE_SURFACE));
            tokens.add(token("lemma" + i, 0, TYPE_LEMMA));
            tokens.add(token("stem" + i, 0, TYPE_STEM));
        }
        assertEquals(variants(phrase("surface", positions), phrase("lemma", positions), phrase("stem", positions)), build(tokens).getVariants());
    }

    /**
     * Change which type is emitted first: LEMMA at position one, STEM at position two. Primary becomes {@code l1 s2}, which differs from both complete type
     * phrases. It must remain alongside {@code l1 l2} and {@code s1 s2}, giving four final clauses when the original is included.
     */
    @Test
    void primaryIsRetainedWhenTheFirstTypeChangesBetweenPositions() throws Exception {
        List<TokenForm> tokens = Arrays.asList(token("l1", 1, TYPE_LEMMA), token("s1", 0, TYPE_STEM), token("s2", 1, TYPE_STEM), token("l2", 0, TYPE_LEMMA));
        assertEquals(variants("l1 s2", "l1 l2", "s1 s2"), build(tokens).getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(tokens)) {
            assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'original', 'words') || " + "content:phrase(TOKFIELD, termOffsetMap, 'l1', 'l2') || "
                            + "content:phrase(TOKFIELD, termOffsetMap, 'l1', 's2') || " + "content:phrase(TOKFIELD, termOffsetMap, 's1', 's2'))",
                            parse(analyzer, "TOKFIELD:\"original words\""));
        }
    }

    /**
     * Supply stems only at positions two and four. The late-created STEM phrase must copy the first position's primary word and use primary again at the
     * missing third position, producing {@code one s2 three s4}. This checks fallback both before and after a type first appears.
     */
    @Test
    void missingModalityUsesPrimaryIncludingBackfill() throws Exception {
        List<TokenForm> tokens = Arrays.asList(token("one", 1, TYPE_LEMMA), token("two", 1, TYPE_LEMMA), token("s2", 0, TYPE_STEM),
                        token("three", 1, TYPE_LEMMA), token("four", 1, TYPE_LEMMA), token("s4", 0, TYPE_STEM));
        assertEquals(variants("one two three four", "one s2 three s4"), build(tokens).getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(tokens)) {
            assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'original', 'words') || "
                            + "content:phrase(TOKFIELD, termOffsetMap, 'one', 's2', 'three', 's4') || "
                            + "content:phrase(TOKFIELD, termOffsetMap, 'one', 'two', 'three', 'four'))", parse(analyzer, "TOKFIELD:\"original words\""));
        }
    }

    /**
     * Supply two different LEMMA forms at the middle position. The additional phrase uses {@code other2} there, but its preceding and following words must come
     * from the LEMMA phrase: {@code l1 other2 l3}. The distinct primary words {@code p1} and {@code p3} expose incorrect primary fallback.
     */
    @Test
    void sameTypeCollisionFallsBackToBaseType() {
        List<TokenForm> tokens = Arrays.asList(token("p1", 1, TYPE_SURFACE), token("l1", 0, TYPE_LEMMA), token("p2", 1, TYPE_SURFACE),
                        token("l2", 0, TYPE_LEMMA), token("other2", 0, TYPE_LEMMA), token("p3", 1, TYPE_SURFACE), token("l3", 0, TYPE_LEMMA));
        assertEquals(variants("p1 p2 p3", "l1 l2 l3", "l1 other2 l3"), build(tokens).getVariants());
    }

    /**
     * Supply a second LEMMA form at each of the first two positions. Those forms share one additional phrase, {@code other1 other2 l3}, with the base form at
     * position three. Mixing first and second forms independently must not create more phrases.
     */
    @Test
    void repeatedCollisionsReuseSpillTracksWithoutCrossProduct() throws Exception {
        List<TokenForm> tokens = Arrays.asList(token("l1", 1, TYPE_LEMMA), token("other1", 0, TYPE_LEMMA), token("l2", 1, TYPE_LEMMA),
                        token("other2", 0, TYPE_LEMMA), token("l3", 1, TYPE_LEMMA));
        assertEquals(variants("l1 l2 l3", "other1 other2 l3"), build(tokens).getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(tokens)) {
            assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'original', 'words') || " + "content:phrase(TOKFIELD, termOffsetMap, 'l1', 'l2', 'l3') || "
                            + "content:phrase(TOKFIELD, termOffsetMap, 'other1', 'other2', 'l3'))", parse(analyzer, "TOKFIELD:\"original words\""));
        }
    }

    /**
     * Supply three forms of one type at the first position and only two at the next. The third phrase must use the next position's first form, producing
     * {@code c d}, while the first and second phrases remain {@code a d} and {@code b e}.
     */
    @Test
    void thirdSameTypeFormGetsItsOwnSpillTrack() {
        List<TokenForm> tokens = Arrays.asList(token("a", 1, TYPE_GENERIC), token("b", 0, TYPE_GENERIC), token("c", 0, TYPE_GENERIC),
                        token("d", 1, TYPE_GENERIC), token("e", 0, TYPE_GENERIC));
        assertEquals(variants("a d", "b e", "c d"), build(tokens).getVariants());
    }

    /**
     * Repeat the identical form {@code a} before emitting a distinct second form {@code b}. Only distinct forms receive numbered phrases, so {@code b} must
     * still align with the second form {@code d} at the next position, producing {@code b d}.
     */
    @Test
    void duplicateSameTypeFormsDoNotConsumeSpillTracks() {
        List<TokenForm> tokens = Arrays.asList(token("a", 1, TYPE_GENERIC), token("a", 0, TYPE_GENERIC), token("b", 0, TYPE_GENERIC),
                        token("c", 1, TYPE_GENERIC), token("d", 0, TYPE_GENERIC));
        assertEquals(variants("a c", "b d"), build(tokens).getVariants());
    }

    /**
     * Use a real type label {@code custom#2} alongside two forms labeled {@code custom}. The real label must stay separate from the additional phrase for
     * {@code custom}'s second form. The three results also show that type names do not have special meanings in the builder.
     */
    @Test
    void arbitraryTypeStringsCannotCollideWithSpillTracks() {
        List<TokenForm> tokens = Arrays.asList(token("a", 1, "custom"), token("b", 0, "custom"), token("c", 0, "custom#2"), token("d", 1, "custom"),
                        token("e", 0, "custom#2"));
        assertEquals(variants("a d", "b d", "c e"), build(tokens).getVariants());
    }

    /**
     * After the alternatives {@code portable} and {@code port}, emit {@code wi} and {@code fi} with positive increments. Both components must appear in every
     * generated phrase as successive words. Splitting the input {@code wi-fi} adds one position, so original slop three becomes four for both generated
     * phrases, while an input without slop still produces exact phrases.
     */
    @Test
    void sequentialComponentsAppendToEveryTrackAndPreserveSlop() throws Exception {
        List<TokenForm> tokens = Arrays.asList(token("portable", 1, TYPE_LEMMA), token("port", 0, TYPE_STEM), token("wi", 1, TYPE_COMPONENT),
                        token("fi", 1, TYPE_COMPONENT));
        assertEquals(variants("portable wi fi", "port wi fi"), build(tokens).getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(tokens)) {
            assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'portable', 'wi-fi') || " + "content:phrase(TOKFIELD, termOffsetMap, 'port', 'wi', 'fi') || "
                            + "content:phrase(TOKFIELD, termOffsetMap, 'portable', 'wi', 'fi'))", parse(analyzer, "TOKFIELD:\"portable wi-fi\""));
            assertEquals("(content:within(TOKFIELD, 3, termOffsetMap, 'portable', 'wi-fi') || "
                            + "content:within(TOKFIELD, 4, termOffsetMap, 'port', 'wi', 'fi') || "
                            + "content:within(TOKFIELD, 4, termOffsetMap, 'portable', 'wi', 'fi'))", parse(analyzer, "TOKFIELD:\"portable wi-fi\"~3"));
        }
    }

    /**
     * Give the second word an increment of two to model a position gap, with LEMMA and STEM alternatives at both word positions. Counting increments gives
     * three positions and adjusts original slop three to four. The existing disabled-increments rule counts all four emitted tokens instead, yielding slop
     * five. Every generated phrase must receive the same adjustment in either mode.
     */
    @Test
    void positionGapsPreserveSlopForEveryModality() throws Exception {
        try (Analyzer analyzer = new SyntheticAnalyzer(
                        Arrays.asList(token("l1", 1, TYPE_LEMMA), token("s1", 0, TYPE_STEM), token("l2", 2, TYPE_LEMMA), token("s2", 0, TYPE_STEM)))) {
            assertEquals("(content:within(TOKFIELD, 3, termOffsetMap, 'first', 'second') || " + "content:within(TOKFIELD, 4, termOffsetMap, 'l1', 'l2') || "
                            + "content:within(TOKFIELD, 4, termOffsetMap, 's1', 's2'))", parse(analyzer, "TOKFIELD:\"first second\"~3"));
            LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
            parser.setAnalyzer(analyzer);
            parser.setPositionIncrementsEnabled(false);
            // Preserve the existing disabled-increments count, which counts every emitted token.
            assertEquals("(content:within(TOKFIELD, 3, termOffsetMap, 'first', 'second') || " + "content:within(TOKFIELD, 5, termOffsetMap, 'l1', 'l2') || "
                            + "content:within(TOKFIELD, 5, termOffsetMap, 's1', 's2'))", parser.parse("TOKFIELD:\"first second\"~3").getOriginalQuery());
        }
    }

    /**
     * Emit exactly one form per position, even though its type label changes. All type phrases then equal primary. The builder must return one string, and the
     * parser must keep the original phrase alone because that string also equals the input text.
     */
    @Test
    void allSingleFormsProduceNoAdditionalVariants() throws Exception {
        List<TokenForm> tokens = Arrays.asList(token("one", 1, "firstType"), token("two", 1, "secondType"), token("three", 1, "firstType"));
        assertEquals(variants("one two three"), build(tokens).getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(tokens)) {
            assertEquals("content:phrase(TOKFIELD, termOffsetMap, 'one', 'two', 'three')", parse(analyzer, "TOKFIELD:\"one two three\""));
        }
    }

    /**
     * Emit {@code ORIGINAL} and {@code stem} for input {@code original}. A case-only difference must not add a clause. Inspect the processed Lucene tree
     * directly to verify that its first clause is the actual original node and its only other clause contains the distinct stem.
     */
    @Test
    void originalNodeStaysFirstAndBaseTextIsSkippedIgnoringCase() throws Exception {
        try (Analyzer analyzer = new SyntheticAnalyzer(Arrays.asList(token("ORIGINAL", 1, TYPE_LEMMA), token("stem", 0, TYPE_STEM)))) {
            CustomAnalyzerQueryNodeProcessor processor = new CustomAnalyzerQueryNodeProcessor();
            QueryConfigHandler config = new QueryConfigHandler();
            config.set(ConfigurationKeys.ANALYZER, analyzer);
            config.set(ConfigurationKeys.ENABLE_POSITION_INCREMENTS, true);
            config.set(LuceneToJexlQueryParser.TOKENIZED_FIELDS, Collections.singleton(TOKENIZED_FIELD));
            processor.setQueryConfigHandler(config);
            QuotedFieldQueryNode original = new QuotedFieldQueryNode(TOKENIZED_FIELD, "original", -1, -1);
            GroupQueryNode group = assertInstanceOf(GroupQueryNode.class, processor.process(original));
            OrQueryNode or = assertInstanceOf(OrQueryNode.class, group.getChild());
            assertEquals(2, or.getChildren().size());
            assertSame(original, or.getChildren().get(0));
            QueryNode variant = or.getChildren().get(1);
            assertEquals("stem", assertInstanceOf(QuotedFieldQueryNode.class, variant).getTextAsString());
        }
    }

    /**
     * Analyze one unquoted input word into lemma and stem alternatives at the same position. Final JEXL must contain three single-word equality clauses,
     * including the original, rather than multi-word phrase expressions.
     */
    @Test
    void unquotedSingleTermVariantsRemainSingleTerms() throws Exception {
        try (Analyzer analyzer = new SyntheticAnalyzer(Arrays.asList(token("lemma", 1, TYPE_LEMMA), token("stem", 0, TYPE_STEM)))) {
            assertEquals("(TOKFIELD == 'original' || TOKFIELD == 'lemma' || TOKFIELD == 'stem')", parse(analyzer, "TOKFIELD:original"));
        }
    }

    /**
     * Split the unquoted input {@code wi-fi} into two successive words. With tokenized-term slop enabled, its alternative allows distance two; disabling that
     * setting makes the alternative an exact phrase. Both outputs must also preserve the original single-word equality clause.
     */
    @Test
    void unquotedSequentialExpansionPreservesSlop() throws Exception {
        try (Analyzer analyzer = new SyntheticAnalyzer(Arrays.asList(token("wi", 1, TYPE_COMPONENT), token("fi", 1, TYPE_COMPONENT)))) {
            assertEquals("(TOKFIELD == 'wi-fi' || content:within(TOKFIELD, 2, termOffsetMap, 'wi', 'fi'))", parse(analyzer, "TOKFIELD:wi-fi"));
            LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
            parser.setAnalyzer(analyzer);
            parser.setUseSlopForTokenizedTerms(false);
            assertEquals("(TOKFIELD == 'wi-fi' || content:phrase(TOKFIELD, termOffsetMap, 'wi', 'fi'))", parser.parse("TOKFIELD:wi-fi").getOriginalQuery());
        }
    }

    /**
     * Omit TypeAttribute entirely while emitting two forms at each word position. The processor must group their unknown types together and align first forms
     * as {@code a c} and second forms as {@code b d}, without assuming a named type or generating all combinations.
     */
    @Test
    void missingTypeAttributeUsesSameTypeSpills() throws Exception {
        try (Analyzer analyzer = new SyntheticAnalyzer(Arrays.asList(token("a", 1, null), token("b", 0, null), token("c", 1, null), token("d", 0, null)), false,
                        true)) {
            assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'original', 'words') || "
                            + "content:phrase(TOKFIELD, termOffsetMap, 'a', 'c') || content:phrase(TOKFIELD, termOffsetMap, 'b', 'd'))",
                            parse(analyzer, "TOKFIELD:\"original words\""));
        }
    }

    /**
     * Omit PositionIncrementAttribute entirely. Although the fixture data records zero increments, that data is not exposed by the stream, so the processor
     * must use its default increment of one and retain {@code a b} as two successive words.
     */
    @Test
    void missingPositionAttributeTreatsTokensAsSequential() throws Exception {
        try (Analyzer analyzer = new SyntheticAnalyzer(Arrays.asList(token("a", 0, "first"), token("b", 0, "second")), true, false)) {
            assertEquals("(TOKFIELD == 'original' || content:within(TOKFIELD, 2, termOffsetMap, 'a', 'b'))", parse(analyzer, "TOKFIELD:original"));
        }
    }

    /**
     * Provide no tokens. The builder must report no variants and return an empty set; the parser must preserve the original query without adding an empty
     * phrase or an unnecessary OR group.
     */
    @Test
    void emptyStreamKeepsOriginal() throws Exception {
        ModalityVariantBuilder builder = new ModalityVariantBuilder();
        assertTrue(builder.hasNoVariants());
        assertEquals(Collections.emptySet(), builder.getVariants());
        try (Analyzer analyzer = new SyntheticAnalyzer(Collections.emptyList())) {
            assertEquals("TOKFIELD == 'original'", parse(analyzer, "TOKFIELD:original"));
        }
    }

    /**
     * Begin with a zero-increment token before any position exists. It still establishes the first position, and a second type adds an alternative there.
     * Repeated reads must return the same sorted set, and starting the next position must append its word to both phrases.
     */
    @Test
    void leadingZeroIncrementStartsFirstPositionAndSnapshotsAreStable() {
        ModalityVariantBuilder builder = new ModalityVariantBuilder();
        builder.append("a", "first", true).append("b", "second", true);
        assertFalse(builder.hasNoVariants());
        assertInstanceOf(TreeSet.class, builder.getVariants());
        assertEquals(variants("a", "b"), builder.getVariants());
        builder.append("c", "first", false);
        assertEquals(variants("a c", "b c"), builder.getVariants());
    }

    /**
     * Read variants partway through a position, then add another form of an existing type and a newly observed type at that same position. These forms must
     * replace words in that position, rather than become an extra word. Continuing to a third position also checks correct fallback, and saved result sets must
     * remain unchanged by later appends.
     */
    @Test
    void readingVariantsDoesNotCloseTheCurrentPosition() {
        ModalityVariantBuilder builder = new ModalityVariantBuilder();
        builder.append("p1", TYPE_SURFACE, false).append("l1", TYPE_LEMMA, true);
        Set<String> firstPosition = builder.getVariants();
        assertEquals(variants("p1", "l1"), firstPosition);

        builder.append("p2", TYPE_SURFACE, false).append("l2", TYPE_LEMMA, true);
        Set<String> secondPosition = builder.getVariants();
        assertEquals(variants("p1 p2", "l1 l2"), secondPosition);

        // A new form of an existing type and a newly observed type still belong to the second position.
        builder.append("other2", TYPE_LEMMA, true).append("s2", TYPE_STEM, true);
        assertEquals(variants("p1 p2", "l1 l2", "l1 other2", "p1 s2"), builder.getVariants());

        builder.append("p3", TYPE_SURFACE, false).append("l3", TYPE_LEMMA, true);
        assertEquals(variants("p1 p2 p3", "l1 l2 l3", "l1 other2 l3", "p1 s2 p3"), builder.getVariants());
        assertEquals(variants("p1", "l1"), firstPosition);
        assertEquals(variants("p1 p2", "l1 l2"), secondPosition);
    }

    /**
     * Exercise the full Lucene-to-JEXL parser with the supplied controlled analyzer and return its final query expression.
     */
    private static String parse(Analyzer analyzer, String query) throws Exception {
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAnalyzer(analyzer);
        return parser.parse(query).getOriginalQuery();
    }

    /**
     * Describe the expected JEXL for a known phrase: equality for one word, or an exact phrase expression for several words.
     */
    private static String clause(String phrase) {
        if (!phrase.contains(" ")) {
            return TOKENIZED_FIELD + " == '" + phrase + "'";
        }
        return "content:phrase(" + TOKENIZED_FIELD + ", termOffsetMap, '" + phrase.replace(" ", "', '") + "')";
    }

    /**
     * Create distinct numbered words such as {@code lemma0 lemma1} for a phrase of the requested length.
     */
    private static String phrase(String prefix, int positions) {
        return IntStream.range(0, positions).mapToObj(i -> prefix + i).collect(Collectors.joining(" "));
    }

    /**
     * Express expected builder results as a sorted set, so duplicate strings and assertion order do not affect comparisons.
     */
    private static Set<String> variants(String... phrases) {
        return new TreeSet<>(Arrays.asList(phrases));
    }

    /**
     * Feed the declared forms directly to the builder for tests focused on phrase construction. Parser tests use the same forms through an actual token stream
     * to check attribute reading and final query output as well.
     */
    private static ModalityVariantBuilder build(List<TokenForm> tokens) {
        ModalityVariantBuilder builder = new ModalityVariantBuilder();
        tokens.forEach(token -> builder.append(token.text, token.type, token.increment == 0));
        return builder;
    }

    /**
     * Declare one emitted form; increment zero makes it an alternative to the preceding word, and a positive increment starts a new word position.
     */
    private static TokenForm token(String text, int increment, String type) {
        return new TokenForm(text, increment, type);
    }

    /**
     * Immutable fixture data for one token emission. Multiple instances may describe different forms at the same position; {@code type} may be null for tests
     * whose streams omit type information.
     */
    private static class TokenForm {
        private final String text;
        private final int increment;
        private final String type;

        private TokenForm(String text, int increment, String type) {
            this.text = text;
            this.increment = increment;
            this.type = type;
        }
    }

    /**
     * Emits the supplied token forms in order, regardless of the input text. Optional attributes let tests exercise streams that have no type or position
     * information.
     */
    private static class SyntheticAnalyzer extends Analyzer {
        private final List<TokenForm> tokens;
        private final boolean includeType;
        private final boolean includePosition;

        /**
         * Expose both type labels and position increments for ordinary test streams.
         */
        private SyntheticAnalyzer(List<TokenForm> tokens) {
            this(tokens, true, true);
        }

        /**
         * Choose which attributes the stream exposes. Omitting an attribute tests the processor's default behavior; it does not merely supply that attribute
         * with an empty value.
         */
        private SyntheticAnalyzer(List<TokenForm> tokens, boolean includeType, boolean includePosition) {
            this.tokens = tokens;
            this.includeType = includeType;
            this.includePosition = includePosition;
        }

        /**
         * Build a tokenizer that copies each declared form into Lucene attributes and resets its index when the analyzer reuses the tokenizer.
         */
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new Tokenizer() {
                private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
                private final TypeAttribute type = includeType ? addAttribute(TypeAttribute.class) : null;
                private final PositionIncrementAttribute position = includePosition ? addAttribute(PositionIncrementAttribute.class) : null;
                private int index;

                @Override
                public boolean incrementToken() {
                    if (index == tokens.size()) {
                        return false;
                    }
                    clearAttributes();
                    TokenForm token = tokens.get(index++);
                    term.append(token.text);
                    if (type != null) {
                        type.setType(token.type);
                    }
                    if (position != null) {
                        position.setPositionIncrement(token.increment);
                    }
                    return true;
                }

                @Override
                public void reset() throws IOException {
                    super.reset();
                    index = 0;
                }
            };
            return new TokenStreamComponents(tokenizer);
        }
    }
}
