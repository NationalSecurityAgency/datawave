package datawave.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

import datawave.util.flag.InMemoryStubFileSystem;

public class FilteringIteratorTest {
    private static final FileStatus FILE_STATUS_ONE = createFileStatusObject("One");
    private static final FileStatus FILE_STATUS_TWO = createFileStatusObject("Two");
    private static final List<FileStatus> TEST_DATA = Arrays.asList(FILE_STATUS_ONE, FILE_STATUS_TWO);

    private static final PathFilter ALWAYS_REJECTS = (path -> false);
    private static final PathFilter ALWAYS_ACCEPTS = (path -> true);
    private static final PathFilter ACCEPTS_ONE = (path -> path.toString().contains("One"));

    private RemoteIterator sourceIteratorWithData;
    private RemoteIterator sourceIteratorWithNoData;
    private RemoteIterator sourceIteratorWithBadData;

    @Before
    public void before() {
        sourceIteratorWithData = new TestRemoteIterator(TEST_DATA.iterator());
        sourceIteratorWithNoData = new TestRemoteIterator(Collections.emptyIterator());
        sourceIteratorWithBadData = new TestRemoteIterator(Collections.singletonList((FileStatus) null).iterator());
    }

    @Test
    public void testFilterAlwaysRejectsData() {
        FilteringIterator<FileStatus> filteringIterator = new FilteringIterator<>(sourceIteratorWithData, ALWAYS_REJECTS);
        assertFalse(filteringIterator.hasNext());
    }

    @Test
    public void testFilterAlwaysAcceptsData() {
        FilteringIterator<FileStatus> filteringIterator = new FilteringIterator<>(sourceIteratorWithData, ALWAYS_ACCEPTS);

        assertNextFileStatus(FILE_STATUS_ONE, filteringIterator);
        assertNextFileStatus(FILE_STATUS_TWO, filteringIterator);

        assertFalse(filteringIterator.hasNext());
    }

    @Test
    public void testFilterSometimesAcceptsData() {
        FilteringIterator<FileStatus> filteringIterator = new FilteringIterator<>(sourceIteratorWithData, ACCEPTS_ONE);
        assertNextFileStatus(FILE_STATUS_ONE, filteringIterator);
        assertFalse(filteringIterator.hasNext());
    }

    @Test
    public void testFilterWithNoData() {
        assertFalse(new FilteringIterator<>(sourceIteratorWithNoData, ACCEPTS_ONE).hasNext());
        assertFalse(new FilteringIterator<>(sourceIteratorWithNoData, ALWAYS_REJECTS).hasNext());
        assertFalse(new FilteringIterator<>(sourceIteratorWithNoData, ALWAYS_ACCEPTS).hasNext());
    }

    @Test
    public void testFilterOverBadData() {
        FilteringIterator filteringIterator = new FilteringIterator<>(sourceIteratorWithBadData, ALWAYS_ACCEPTS);
        assertFalse(filteringIterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElement() {
        FilteringIterator filteringIterator = new FilteringIterator<>(sourceIteratorWithBadData, ALWAYS_ACCEPTS);
        assertFalse(filteringIterator.hasNext());
        filteringIterator.next();
    }

    @Test(expected = RuntimeException.class)
    public void testIOExceptionHandling() {
        RemoteIterator<FileStatus> exceptionThrowingRemoteIterator = new RemoteIterator<>() {
            @Override
            public boolean hasNext() throws IOException {
                return true;
            }

            @Override
            public FileStatus next() throws IOException {
                throw new IOException();
            }
        };
        new FilteringIterator<>(exceptionThrowingRemoteIterator, ACCEPTS_ONE).hasNext();
    }

    private void assertNextFileStatus(FileStatus expectedFileStatus, FilteringIterator<FileStatus> filteringIterator) {
        assertTrue(filteringIterator.hasNext());
        assertEquals(expectedFileStatus, filteringIterator.next());
    }

    private static FileStatus createFileStatusObject(String name) {
        return InMemoryStubFileSystem.stubFileStatus(new Path(name));
    }

    // Converts Iterator into RemoteIterator
    public static class TestRemoteIterator implements RemoteIterator<FileStatus> {

        private final Iterator<FileStatus> iterator;

        public TestRemoteIterator(Iterator<FileStatus> itemIterator) {
            this.iterator = itemIterator;
        }

        @Override
        public boolean hasNext() throws IOException {
            return iterator.hasNext();
        }

        @Override
        public FileStatus next() throws IOException {
            return iterator.next();
        }
    };
}
