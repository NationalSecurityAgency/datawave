package datawave.core.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class DataWaveFieldIndexRangeIteratorJexlTest {
    
    @Test
    public void nonnegatedRangesTest() {
        SortedSet<Range> sortedRanges = new TreeSet<>();
        
        sortedRanges.add(new Range(new Key("A"), false, new Key("C"), false));
        sortedRanges.add(new Range(new Key("E"), false, new Key("G"), true));
        sortedRanges.add(new Range(new Key("J"), true, new Key("L"), false));
        sortedRanges.add(new Range(new Key("O"), true, new Key("R"), true));
        sortedRanges.add(new Range(new Key("T"), new Key("T")));
        
        TestDataWaveFieldIndexRangeIteratorJexl rangeIvarator = new TestDataWaveFieldIndexRangeIteratorJexl(sortedRanges, false);
        
        List<Range> boundingFiRanges = rangeIvarator.buildBoundingFiRanges(new Text("20190424"), new Text("fi\0fieldName"), null);
        
        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "A\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "B\uDBFF\uDFFF"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "E\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "G\u0001"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "J\u0000"), true, new Key("20190424", "fi\0fieldName", "K\uDBFF\uDFFF"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "O\u0000"), true, new Key("20190424", "fi\0fieldName", "R\u0001"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "T\u0000"), true, new Key("20190424", "fi\0fieldName", "T\u0001"), true));
        
        Assert.assertEquals(expectedRanges.size(), boundingFiRanges.size());
        
        for (int i = 0; i < expectedRanges.size(); i++)
            Assert.assertEquals(expectedRanges.get(i), boundingFiRanges.get(i));
    }
    
    @Test
    public void negatedRangesTest() {
        SortedSet<Range> sortedRanges = new TreeSet<>();
        
        sortedRanges.add(new Range(new Key("A"), false, new Key("C"), false));
        sortedRanges.add(new Range(new Key("E"), false, new Key("G"), true));
        sortedRanges.add(new Range(new Key("J"), true, new Key("L"), false));
        sortedRanges.add(new Range(new Key("O"), true, new Key("R"), true));
        sortedRanges.add(new Range(new Key("T"), new Key("T")));
        
        TestDataWaveFieldIndexRangeIteratorJexl rangeIvarator = new TestDataWaveFieldIndexRangeIteratorJexl(sortedRanges, true);
        
        List<Range> boundingFiRanges = rangeIvarator.buildBoundingFiRanges(new Text("20190424"), new Text("fi\0fieldName"), null);
        
        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName"), true, new Key("20190424", "fi\0fieldName", "A\u0001"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "C\u0000"), true, new Key("20190424", "fi\0fieldName", "E\u0001"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "G\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "I\uDBFF\uDFFF"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "L\u0000"), true, new Key("20190424", "fi\0fieldName", "N\uDBFF\uDFFF"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "R\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "S\uDBFF\uDFFF"), true));
        expectedRanges.add(new Range(new Key("20190424", "fi\0fieldName", "T\u0001\u0000"), true, new Key("20190424", "fi\0fieldName\0"), true));
        
        Assert.assertEquals(expectedRanges.size(), boundingFiRanges.size());
        
        for (int i = 0; i < expectedRanges.size(); i++)
            Assert.assertEquals(expectedRanges.get(i), boundingFiRanges.get(i));
    }
    
    private static class TestDataWaveFieldIndexRangeIteratorJexl extends DatawaveFieldIndexRangeIteratorJexl {
        
        boolean negated = false;
        
        public TestDataWaveFieldIndexRangeIteratorJexl(SortedSet<Range> subRanges, boolean negated) {
            this.subRanges = subRanges;
            this.negated = negated;
        }
        
        public List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue) {
            return super.buildBoundingFiRanges(rowId, fiName, fieldValue);
        }
        
        @Override
        public boolean isNegated() {
            return negated;
        }
    }
}
