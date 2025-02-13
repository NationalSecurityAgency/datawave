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
        TermWeightPosition twp = twpBuilder.setOffset(4).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(.65F).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(2).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(3).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        termWeightPositionList.add(twp);

        // Simple ordering tests with prevSkips
        twp = twpBuilder.setOffset(12).setPrevSkips(1).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(.65F).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(13).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).setLeftBound(.65F).setRightBound(1.7F).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(11).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(1.7F).setRightBound(8.3F).build();
        termWeightPositionList.add(twp);
        twp = twpBuilder.setOffset(15).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).setLeftBound(8.3F).setRightBound(10F).build();
        termWeightPositionList.add(twp);

        termWeightPositionList = Collections.unmodifiableList(termWeightPositionList);
    }

    @Test
    public void testMaxOffsetComparator() {
        List<TermWeightPosition> listExpected = new ArrayList<>();
        TermWeightPosition.Builder twpBuilder = new TermWeightPosition.Builder();
        TermWeightPosition twp;

        // Simple ordering tests
        twp = twpBuilder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(2).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(3).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(4).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);

        // Simple ordering tests with prevSkips
        twp = twpBuilder.setOffset(11).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(1.7F).setRightBound(8.3F).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(12).setPrevSkips(1).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(.65F).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(13).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).setLeftBound(.65F).setRightBound(1.7F).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(15).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).setLeftBound(8.3F).setRightBound(10F).build();
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
        twp = twpBuilder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(2).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(3).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(4).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        listExpected.add(twp);

        // Simple ordering tests with prevSkips
        twp = twpBuilder.setOffset(13).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).setLeftBound(.65F).setRightBound(1.7F).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(11).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(1.7F).setRightBound(8.3F).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(12).setPrevSkips(1).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(.65F).build();
        listExpected.add(twp);
        twp = twpBuilder.setOffset(15).setPrevSkips(4).setScore(0).setZeroOffsetMatch(true).setLeftBound(8.3F).setRightBound(10F).build();
        listExpected.add(twp);

        List<TermWeightPosition> result = new ArrayList<>(termWeightPositionList);
        Collections.sort(result);
        assertEquals(listExpected, result);
    }

    @Test
    public void testBuilderReset() {
        TermWeightPosition.Builder builder = new TermWeightPosition.Builder();

        TermWeightPosition expectedAfterReset = builder.setOffset(-1).setPrevSkips(-1).setScore(-1).setZeroOffsetMatch(false).setLeftBound(-1).setRightBound(-1).build();

        TermWeightPosition expected = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        TermWeightPosition position = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        assertEquals(expected, position);

        expected = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        builder.reset();
        assertEquals(expectedAfterReset, builder.build());
        position = builder.setOffset(1).setPrevSkips(0).setScore(0).setZeroOffsetMatch(true).setLeftBound(0).setRightBound(0).build();
        assertEquals(expected, position);
    }

    @Test
    public void testPositionScoreToTermWeightScore() {
        float positionScore = -.0552721F;
        int twScore = TermWeightPosition.positionScoreToTermWeightScore(positionScore);
        float result = TermWeightPosition.termWeightScoreToPositionScore(twScore);

        assertEquals(result + "!=" + positionScore, positionScore, result, 0);
    }
}
