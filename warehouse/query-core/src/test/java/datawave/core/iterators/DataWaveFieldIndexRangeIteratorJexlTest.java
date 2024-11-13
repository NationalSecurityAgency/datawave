package datawave.core.iterators;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class DataWaveFieldIndexRangeIteratorJexlTest {
    //  @formatter:off
    private final SortedSet<Range> sortedRanges = new TreeSet<>(List.of(
                    (new Range(new Key("A"), false, new Key("C"), false)),
                    (new Range(new Key("E"), false, new Key("G"), true)),
                    (new Range(new Key("J"), true, new Key("L"), false)),
                    (new Range(new Key("O"), true, new Key("R"), true)),
                    (new Range(new Key("T"), new Key("T")))));
    //  @formatter:on

    @Test
    public void nonnegatedRangesTest() {

        final List<Range> ExpectedRanges = new ArrayList<>(List.of(
                        (new Range(new Key("20190424", "fi\0fieldName", "A\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "B\uDBFF\uDFFF"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "E\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "G\u0001"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "J\u0000"), true, new Key("20190424", "fi\0fieldName", "K\uDBFF\uDFFF"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "O\u0000"), true, new Key("20190424", "fi\0fieldName", "R\u0001"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "T\u0000"), true, new Key("20190424", "fi\0fieldName", "T\u0001"), true))));

        List<Range> boundingFiRanges = createBoundingFiRanges(false);
        assertEquals(ExpectedRanges.size(), boundingFiRanges.size());

        for (int i = 0; i < ExpectedRanges.size(); i++) {
            assertEquals(ExpectedRanges.get(i), boundingFiRanges.get(i));
        }
    }

    @Test
    public void negatedRangesTest() {

        final List<Range> ExpectedRanges = new ArrayList<>(List.of(
                        (new Range(new Key("20190424", "fi\0fieldName"), true, new Key("20190424", "fi\0fieldName", "A\u0001"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "C\u0000"), true, new Key("20190424", "fi\0fieldName", "E\u0001"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "G\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "I\uDBFF\uDFFF"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "L\u0000"), true, new Key("20190424", "fi\0fieldName", "N\uDBFF\uDFFF"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "R\u0001\u0000"), true, new Key("20190424", "fi\0fieldName", "S\uDBFF\uDFFF"), true)),
                        (new Range(new Key("20190424", "fi\0fieldName", "T\u0001\u0000"), true, new Key("20190424", "fi\0fieldName\0"), true))));

        List<Range> boundingFiRanges = createBoundingFiRanges(true);
        assertEquals(ExpectedRanges.size(), boundingFiRanges.size());

        for (int i = 0; i < ExpectedRanges.size(); i++) {
            assertEquals(ExpectedRanges.get(i), boundingFiRanges.get(i));
        }
    }

    private List<Range> createBoundingFiRanges(boolean negatedBool) {
        TestDataWaveFieldIndexRangeIteratorJexl rangeIvarator = new TestDataWaveFieldIndexRangeIteratorJexl(sortedRanges, negatedBool);
        return rangeIvarator.buildBoundingFiRanges(new Text("20190424"), new Text("fi\0fieldName"), null);
    }

    private static class TestDataWaveFieldIndexRangeIteratorJexl extends DatawaveFieldIndexRangeIteratorJexl {

        boolean negated;

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
