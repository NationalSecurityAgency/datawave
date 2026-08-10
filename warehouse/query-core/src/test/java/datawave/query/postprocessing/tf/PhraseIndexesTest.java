package datawave.query.postprocessing.tf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;

public class PhraseIndexesTest {

    private static final String EVENT_ID_1 = "shard1\u0000dt\u0000uid1";
    private static final String EVENT_ID_2 = "shard2\u0000dt\u0000uid2";

    /**
     * Verify formatting an empty {@link PhraseIndexes} returns an empty string.
     */
    @Test
    public void testEmptyPhraseOffsetsToString() {
        assertEquals("", new PhraseIndexes().toString());
    }

    /**
     * Verify formatting a non-empty {@link PhraseIndexes} to a string.
     */
    @Test
    public void testNonEmptyPhraseOffsetsToString() {
        PhraseIndexes phraseIndexes = new PhraseIndexes();
        phraseIndexes.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        phraseIndexes.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        phraseIndexes.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        phraseIndexes.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);

        assertEquals("BODY:" + EVENT_ID_1 + ",1,3:" + EVENT_ID_2 + ",10,11/CONTENT:" + EVENT_ID_1 + ",3,4:" + EVENT_ID_2 + ",12,17", phraseIndexes.toString());
    }

    /**
     * Verify offsets are ordered correctly.
     */
    @Test
    public void testReorderedOffsets() {
        PhraseIndexes phraseIndexes = new PhraseIndexes();
        phraseIndexes.addIndexTriplet("BODY", EVENT_ID_1, 3, 1);
        phraseIndexes.addIndexTriplet("BODY", EVENT_ID_2, 11, 10);
        phraseIndexes.addIndexTriplet("CONTENT", EVENT_ID_1, 4, 3);
        phraseIndexes.addIndexTriplet("CONTENT", EVENT_ID_2, 17, 12);

        assertEquals("BODY:" + EVENT_ID_1 + ",1,3:" + EVENT_ID_2 + ",10,11/CONTENT:" + EVENT_ID_1 + ",3,4:" + EVENT_ID_2 + ",12,17", phraseIndexes.toString());
    }

    /**
     * Verify that {@link PhraseIndexes#from(String)} returns null when given a null input.
     */
    @Test
    public void testParsingFromNullString() {
        assertNull(PhraseIndexes.from(null));
    }

    /**
     * Verify that {@link PhraseIndexes#from(String)} returns a non-null, empty {@link PhraseIndexes} from a blank string.
     */
    @Test
    public void testParsingFromEmpytString() {
        assertTrue(PhraseIndexes.from("   ").isEmpty());
    }

    /**
     * Verify that {@link PhraseIndexes#from(String)} correctly parses a non-blank string.
     */
    @Test
    public void testParsingFromNonBlankString() {
        PhraseIndexes expected = new PhraseIndexes();
        expected.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        expected.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        expected.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        expected.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);

        PhraseIndexes actual = PhraseIndexes
                        .from("BODY:" + EVENT_ID_1 + ",1,3:" + EVENT_ID_2 + ",10,11/CONTENT:" + EVENT_ID_1 + ",3,4:" + EVENT_ID_2 + ",12,17");
        assertEquals(expected, actual);
    }

    @Test
    public void testOverlappingPhraseOffsets() {
        PhraseIndexes expected = new PhraseIndexes();
        expected.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        expected.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        expected.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        expected.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);

        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 2);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 2, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 11, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 4, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 13);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 14, 15);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 15, 17);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 13, 14);

        assertEquals(expected, actual);
    }

    @Test
    public void testOverlap() {
        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 5, 8);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 19, 20);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);

        assertNull(actual.getOverlap("BODY", EVENT_ID_1, new TermWeightPosition.Builder().setOffset(4).build()));
        assertNull(actual.getOverlap("BODY", EVENT_ID_1, new TermWeightPosition.Builder().setOffset(4).build()));
        assertEquals(PhraseOffset.with(EVENT_ID_1, 1, 3),
                        actual.getOverlap("BODY", EVENT_ID_1, new TermWeightPosition.Builder().setOffset(4).setPrevSkips(1).build()));
        assertEquals(PhraseOffset.with(EVENT_ID_2, 19, 20),
                        actual.getOverlap("CONTENT", EVENT_ID_2, new TermWeightPosition.Builder().setOffset(21).setPrevSkips(3).build()));
        assertEquals(PhraseOffset.with(EVENT_ID_2, 12, 17),
                        actual.getOverlap("CONTENT", EVENT_ID_2, new TermWeightPosition.Builder().setOffset(21).setPrevSkips(4).build()));
    }

    @Test
    public void testGetOverlappingPosition() {
        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 5, 8);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 19, 20);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);

        TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
        builder.addTermOffset(4);
        builder.addPrevSkips(1);
        builder.addScore(1);
        builder.addTermOffset(9);
        builder.addPrevSkips(0);
        builder.addScore(1);
        builder.addTermOffset(18);
        builder.addPrevSkips(0);
        builder.addScore(1);
        builder.addTermOffset(21);
        builder.addPrevSkips(3);
        builder.addScore(1);
        builder.addTermOffset(40);
        builder.addPrevSkips(0);
        builder.addScore(1);
        TermWeight.Info twInfo = builder.build();

        // position 3,4 (4 prevSkip 1) overlaps 1,3 phrase
        assertEquals(new TermWeightPosition.Builder().setOffset(4).setPrevSkips(1).build(), actual.getOverlappingPosition("BODY", EVENT_ID_1, twInfo));
        // no overlaps with 10,11 phrase
        assertNull(actual.getOverlappingPosition("BODY", EVENT_ID_2, twInfo));
        // position 3,4 (4 prevSkip 1) overlaps 3,4 phrase
        assertEquals(new TermWeightPosition.Builder().setOffset(4).setPrevSkips(1).build(), actual.getOverlappingPosition("CONTENT", EVENT_ID_1, twInfo));
        // position 18,21 (21 prevSkip 3) overlaps 19,20 phrase
        assertEquals(new TermWeightPosition.Builder().setOffset(21).setPrevSkips(3).build(), actual.getOverlappingPosition("CONTENT", EVENT_ID_2, twInfo));
    }

    @Test
    public void testGetOverlappingPositionBruteForce() {
        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 5, 8);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 19, 20);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);

        TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
        builder.addTermOffset(9);
        builder.addPrevSkips(0);
        builder.addScore(1);
        builder.addTermOffset(40);
        builder.addPrevSkips(0);
        builder.addScore(1);
        builder.addTermOffset(18);
        builder.addPrevSkips(0);
        builder.addScore(1);
        builder.addTermOffset(4);
        builder.addPrevSkips(1);
        builder.addScore(1);
        builder.addTermOffset(21);
        builder.addPrevSkips(3);
        builder.addScore(1);
        TermWeight.Info twInfo = builder.build();

        // position 3,4 (4 prevSkip 1) overlaps 1,3 phrase
        assertEquals(new TermWeightPosition.Builder().setOffset(4).setPrevSkips(1).build(), actual.getOverlappingPosition("BODY", EVENT_ID_1, twInfo));
        // no overlaps with 10,11 phrase
        assertNull(actual.getOverlappingPosition("BODY", EVENT_ID_2, twInfo));
        // position 3,4 (4 prevSkip 1) overlaps 3,4 phrase
        assertEquals(new TermWeightPosition.Builder().setOffset(4).setPrevSkips(1).build(), actual.getOverlappingPosition("CONTENT", EVENT_ID_1, twInfo));
        // position 18,21 (21 prevSkip 3) overlaps 19,20 phrase
        assertEquals(new TermWeightPosition.Builder().setOffset(21).setPrevSkips(3).build(), actual.getOverlappingPosition("CONTENT", EVENT_ID_2, twInfo));
    }

}
