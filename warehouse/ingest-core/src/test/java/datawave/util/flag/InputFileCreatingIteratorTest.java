package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class InputFileCreatingIteratorTest {

    private InputFileCreatingIterator iteratorWithSource(Iterator<LocatedFileStatus> sourceIterator) {
        return new InputFileCreatingIterator(sourceIterator, null, "baseDir", true);
    }

    @Test
    public void createsExpectedInputFile() {
        InputFileCreatingIterator iterator = iteratorWithSource(new ReturnsItemOnlyOnceButAlwaysClaimsHasNext());

        assertTrue(iterator.hasNext());

        InputFile result = iterator.next();
        assertNotNull(result);
        assertNull(result.getFolder());
        assertEquals(InMemoryStubFileSystem.BLOCK_SIZE, result.getBlocksize());
        assertEquals("baseDir/inputFolder", result.getDirectory());
        assertEquals(InMemoryStubFileSystem.MODIFICATION_TIME, result.getTimestamp());
    }

    @Test
    public void delegatesNext() {
        InputFileCreatingIterator iterator = iteratorWithSource(new ReturnsItemOnlyOnceButAlwaysClaimsHasNext());
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertTrue(iterator.hasNext());
    }

    @Test
    public void delegatesHasNext() {
        assertFalse(iteratorWithSource(new EmptySource()).hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsNoSuchElementExceptionWhenSourceReturnsNull() {
        InputFileCreatingIterator iterator = iteratorWithSource(new EmptySource());
        assertNotNull(iterator.next());
        iterator.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsExceptionWhenDelegateDoesNotHaveNext() {
        iteratorWithSource(new EmptySource()).next();
    }

    private static class ReturnsItemOnlyOnceButAlwaysClaimsHasNext implements Iterator<LocatedFileStatus> {
        LocatedFileStatus item;

        public ReturnsItemOnlyOnceButAlwaysClaimsHasNext() {
            item = InMemoryStubFileSystem.stubFileStatus(new Path("baseDir/inputFolder/file.txt"));
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public LocatedFileStatus next() {
            LocatedFileStatus result = item;
            item = null;
            return result;
        }
    }

    private static class EmptySource implements Iterator<LocatedFileStatus> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public LocatedFileStatus next() {
            return null;
        }
    }
}
