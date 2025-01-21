package datawave.ingest.protobuf;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TermWeightPositionTest {

    private List<TermWeightPosition> termWeightPositionList = new ArrayList<>();

    @Before
    public void setup() {
        TermWeightPosition.Builder twpBuilder = new TermWeightPosition.Builder();

        // Simple ordering tests
        TermWeightPosition twp = twpBuilder.setOffset(4).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(2).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(3).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);

        // Simple ordering tests with prevSkips
        twp = twpBuilder.setOffset(12).setPrevSkips(1).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(13).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(11).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(15).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).build();
        termWeightPositionList.add(twp);

        termWeightPositionList = Collections.unmodifiableList(termWeightPositionList);
    }

    @Test
    public void testMaxOffsetComparator() {
        List<TermWeightPosition> listExpected = new ArrayList<>();
        TermWeightPosition.Builder twpBuilder = new TermWeightPosition.Builder();
        TermWeightPosition twp;

        // Simple ordering tests
        twp = twpBuilder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(2).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(3).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(4).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);

        // Simple ordering tests with prevSkips
        twp = twpBuilder.setOffset(11).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(12).setPrevSkips(1).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(13).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(15).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);

        List<TermWeightPosition> result = new ArrayList<>(termWeightPositionList);
        result.sort(new TermWeightPosition.MaxOffsetComparator());
        assertEquals(listExpected, result);

    }

    @Test
    public void testComparator() {
        List<TermWeightPosition> listExpected = new ArrayList<>();
        TermWeightPosition.Builder twpBuilder = new TermWeightPosition.Builder();
        TermWeightPosition twp;

        // Simple ordering tests
        twp = twpBuilder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(2).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(3).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(4).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);

        // Simple ordering tests with prevSkips
        twp = twpBuilder.setOffset(13).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(11).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(12).setPrevSkips(1).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(15).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).build();
        listExpected.add(twp);

        List<TermWeightPosition> result = new ArrayList<>(termWeightPositionList);
        Collections.sort(result);
        assertEquals(listExpected, result);
    }

    @Test
    public void testBuilderReset() {
        TermWeightPosition.Builder builder = new TermWeightPosition.Builder();
        TermWeightPosition expected = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        TermWeightPosition position = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        assertEquals(expected, position);

        expected = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        builder.reset();
        position = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).build();
        assertEquals(expected, position);

        expected = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(false).build();
        builder.reset();
        position = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(false).build();
        assertEquals(expected, position);
    }

    @Test
    public void testPositionScoreToTermWeightScore() {
        float positionScore = (float) -.0552721;
        int twScore = TermWeightPosition.positionScoreToTermWeightScore(positionScore);
        float result = TermWeightPosition.termWeightScoreToPositionScore(twScore);

        assertEquals(result + "!=" + positionScore, positionScore, result, 0);
    }
}
