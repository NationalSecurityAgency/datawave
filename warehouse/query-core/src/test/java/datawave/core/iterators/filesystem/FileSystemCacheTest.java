package datawave.core.iterators.filesystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSystemCacheTest {

    @TempDir
    Path tempDir;

    private String writeSiteConfig(String name) throws IOException {
        Path file = tempDir.resolve(name);
        String xml = "<?xml version=\"1.0\"?><configuration><property><name>fs.defaultFS</name><value>file:///</value></property></configuration>";
        Files.write(file, xml.getBytes(StandardCharsets.UTF_8));
        return file.toUri().toURL().toString();
    }

    @Test
    void sameConfigReturnsSameInstance() throws Exception {
        String config = writeSiteConfig("core-site.xml");
        FileSystemCache first = FileSystemCache.getInstance(config);
        // a distinct but equal String must still hit the cache
        FileSystemCache second = FileSystemCache.getInstance(new String(config));
        assertSame(first, second);
    }

    @Test
    void differentConfigsReturnDifferentInstances() throws Exception {
        String a = writeSiteConfig("a-site.xml");
        String b = writeSiteConfig("b-site.xml");
        assertNotSame(FileSystemCache.getInstance(a), FileSystemCache.getInstance(b));
        assertNotSame(FileSystemCache.getInstance(a), FileSystemCache.getInstance(a + "," + b));
    }

    @Test
    void nullAndEmptyShareAnInstance() throws Exception {
        assertSame(FileSystemCache.getInstance(null), FileSystemCache.getInstance(""));
    }

    @Test
    void constructorCreatesUnsharedInstance() throws Exception {
        String config = writeSiteConfig("core-site.xml");
        assertNotSame(FileSystemCache.getInstance(config), new FileSystemCache(config));
    }

    @Test
    void malformedUrlIsThrownAndNotCached() {
        assertThrows(MalformedURLException.class, () -> FileSystemCache.getInstance("not-a-url"));
        assertThrows(MalformedURLException.class, () -> FileSystemCache.getInstance("not-a-url"));
    }

    @Test
    void resolvesFileSystemFromConfiguredDefault() throws Exception {
        FileSystemCache cache = FileSystemCache.getInstance(writeSiteConfig("core-site.xml"));
        // no scheme or authority, so the configured fs.defaultFS applies
        FileSystem fs = cache.getFileSystem(new URI("/some/path"));
        assertEquals(LocalFileSystem.class, fs.getClass());
        assertSame(fs, cache.getFileSystem(tempDir.toUri()));
    }

    @Test
    void concurrentCallersShareOneInstance() throws Exception {
        String config = writeSiteConfig("core-site.xml");
        int threads = 16;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<FileSystemCache>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                Callable<FileSystemCache> task = () -> {
                    start.await();
                    return FileSystemCache.getInstance(config);
                };
                futures.add(executor.submit(task));
            }
            start.countDown();

            Set<FileSystemCache> instances = new HashSet<>();
            for (Future<FileSystemCache> future : futures) {
                instances.add(future.get());
            }
            assertEquals(1, instances.size());
        } finally {
            executor.shutdownNow();
        }
    }
}
