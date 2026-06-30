package datawave.scan;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.ScannerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to track and close instances of {@link ScannerBase}, typically a {@link org.apache.accumulo.core.client.Scanner} or
 * {@link org.apache.accumulo.core.client.BatchScanner}
 */
public class ScanManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ScanManager.class);

    protected final Set<ScannerBase> baseInstances = Collections.synchronizedSet(new HashSet<>());

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    private final Object scannerLock = new Object();

    public ScanManager() {
        // empty constructor
    }

    /**
     * Adds a {@link ScannerBase}
     *
     * @param scanner
     *            a ScannerBase instance
     */
    public void addScanner(ScannerBase scanner) {
        synchronized (scannerLock) {
            if (closed.get()) {
                log.warn("ScanManager closed, closing scanner");
                scanner.close();
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Adding scanner {}", scanner);
                }
                this.baseInstances.add(scanner);
            }
        }
    }

    /**
     * Closes a {@link ScannerBase} if it is tracked
     *
     * @param scanner
     *            a ScannerBase
     */
    public void close(ScannerBase scanner) {
        if (baseInstances.remove(scanner)) {
            scanner.close();
            if (log.isDebugEnabled()) {
                log.debug("Closed scanner {}", scanner);
            }
        } else {
            log.warn("ScannerManager was asked to close untracked scanner base: {}", scanner);
        }
    }

    /**
     * Closes all scanners tracked by the {@link ScanManager}
     */
    @Override
    public void close() {
        synchronized (scannerLock) {
            log.trace("ScannerManager asked to close all tracked scanners");
            closed.set(true);

            for (ScannerBase scanner : new HashSet<>(baseInstances)) {
                close(scanner);
            }
        }
    }
}
