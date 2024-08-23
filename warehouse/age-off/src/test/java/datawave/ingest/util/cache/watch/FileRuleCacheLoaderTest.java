package datawave.ingest.util.cache.watch;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;

public class FileRuleCacheLoaderTest {
    @Test
    public void testReloadReturnsNewInstanceWhenChanged() throws Exception {
        String path = "file:/path/to/file";
        FileRuleCacheLoader loader = new FileRuleCacheLoader();
        FileRuleCacheValue val = mock(FileRuleCacheValue.class);
        when(val.hasChanges()).thenReturn(true);
        ListenableFuture<FileRuleCacheValue> reloadedVal = loader.reload(path, val);

        assertNotSame(val, reloadedVal.get());
    }

    @Test
    public void testReloadReturnsSameInstanceWhenNotChanged() throws Exception {
        String path = "file:/path/to/file";
        FileRuleCacheLoader loader = new FileRuleCacheLoader();
        FileRuleCacheValue val = mock(FileRuleCacheValue.class);
        when(val.hasChanges()).thenReturn(false);
        ListenableFuture<FileRuleCacheValue> reloadedVal = loader.reload(path, val);

        assertSame(val, reloadedVal.get());
    }

    @Test
    public void testLoadWillCreateNewInstance() throws Exception {
        String path = "file:/path/to/file";
        FileRuleCacheLoader loader = new FileRuleCacheLoader();
        FileRuleCacheValue loadedVal = loader.load(path);

        assertEquals(path, loadedVal.getFilePath().toString());
    }
}
