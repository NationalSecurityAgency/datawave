package datawave.ingest.protobuf;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TermWeightPositionTest {
    
    private List<TermWeightPosition> termWeightPositionList = Lists.newArrayList();
    
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
        List<TermWeightPosition> listExpected = Lists.newArrayList();
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
        
        List<TermWeightPosition> result = Lists.newArrayList(termWeightPositionList);
        Collections.sort(result, new TermWeightPosition.MaxOffsetComparator());
        Assert.assertEquals(listExpected, result);
        
    }
    
    @Test
    public void testComparator() {
        List<TermWeightPosition> listExpected = Lists.newArrayList();
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
        
        List<TermWeightPosition> result = Lists.newArrayList(termWeightPositionList);
        Collections.sort(result);
        Assert.assertEquals(listExpected, result);
    }
    
    @Test
    public void testPositionScoreToTermWeightScore() {
        Float positionScore = new Float(-.0552721);
        Integer twScore = TermWeightPosition.positionScoreToTermWeightScore(positionScore);
        Float result = TermWeightPosition.termWeightScoreToPositionScore(twScore);
        
        Assert.assertEquals(result + "!=" + positionScore, positionScore, result);
    }
}
