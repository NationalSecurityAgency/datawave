package datawave.common.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class MultiComparatorTest {
    
    private static final TestData[] TEST_DATA = {new TestData(3, "c"), new TestData(3, "b"), new TestData(3, "a"), new TestData(1, "a"), new TestData(1, "b"),
            new TestData(1, "d"), new TestData(2, "c"), new TestData(2, "e"), new TestData(2, "d"),};
    
    @Test
    public void emptyMultiComparatorTest() {
        TestData[] testData = new TestData[TEST_DATA.length];
        System.arraycopy(TEST_DATA, 0, testData, 0, TEST_DATA.length);
        Arrays.sort(testData, new MultiComparator<>());
        assertEquals("[[3, c], [3, b], [3, a], [1, a], [1, b], [1, d], [2, c], [2, e], [2, d]]", Arrays.deepToString(testData));
    }
    
    @Test
    public void intFirstMultiComparatorTest() {
        TestData[] testData = new TestData[TEST_DATA.length];
        System.arraycopy(TEST_DATA, 0, testData, 0, TEST_DATA.length);
        Arrays.sort(testData, new MultiComparator<>(new IntFirstComparator(), new StringFirstComparator()));
        assertEquals("[[1, a], [1, b], [1, d], [2, c], [2, d], [2, e], [3, a], [3, b], [3, c]]", Arrays.deepToString(testData));
        
    }
    
    @Test
    public void stringFirstMultiComparatorTest() {
        TestData[] testData = new TestData[TEST_DATA.length];
        System.arraycopy(TEST_DATA, 0, testData, 0, TEST_DATA.length);
        Arrays.sort(testData, new MultiComparator<>(new StringFirstComparator(), new IntFirstComparator()));
        assertEquals("[[1, a], [3, a], [1, b], [3, b], [2, c], [3, c], [1, d], [2, d], [2, e]]", Arrays.deepToString(testData));
    }
    
    private static class TestData {
        public int myInt;
        public String myString;
        
        public TestData(int myInt, String myString) {
            this.myInt = myInt;
            this.myString = myString;
        }
        
        @Override
        public String toString() {
            return "[" + myInt + ", " + myString + "]";
        }
    }
    
    private static class IntFirstComparator implements Comparator<TestData> {
        @Override
        public int compare(TestData o1, TestData o2) {
            return o1.myInt - o2.myInt;
        }
    }
    
    private static class StringFirstComparator implements Comparator<TestData> {
        @Override
        public int compare(TestData o1, TestData o2) {
            return o1.myString.compareTo(o2.myString);
        }
    }
}
