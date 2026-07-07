package datawave.query.tables;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.scan.ScanManager;

/**
 * An extension of {@link ScanManager} that also tracks {@link ScannerSession} instances in addition to {@link org.apache.accumulo.core.client.Scanner} and
 * {@link org.apache.accumulo.core.client.BatchScanner}
 */
public class ScanSessionManager extends ScanManager {

    private static final Logger log = LoggerFactory.getLogger(ScanSessionManager.class);

    protected final Set<ScannerSession> sessionInstances = Collections.synchronizedSet(new HashSet<>());

    protected final AtomicBoolean sessionsClosed = new AtomicBoolean(false);

    private final Object sessionLock = new Object();

    public ScanSessionManager() {
        // empty constructor
    }

    /**
     * Adds a {@link ScannerSession}
     *
     * @param scanner
     *            a ScannerSession instance
     */
    public void addScanner(ScannerSession scanner) {
        synchronized (sessionLock) {
            if (sessionsClosed.get()) {
                log.warn("ScanSessionManager closed, closing ScannerSession");
                scanner.close();
            } else {
                log.trace("Adding scanner {}", scanner);
                this.sessionInstances.add(scanner);
            }
        }
    }

    /**
     * Closes a {@link ScannerSession} if it is tracked
     *
     * @param scanner
     *            a ScannerSession
     */
    public void close(ScannerSession scanner) {
        if (sessionInstances.remove(scanner)) {
            scanner.close();
            log.debug("Closed scanner: {}", scanner);
        } else {
            log.warn("ScannerManager was asked to close untracked scanner session: {}", scanner);
        }
    }

    /**
     * Closes all scanners tracked by the {@link ScanSessionManager}
     */
    @Override
    public void close() {
        synchronized (sessionLock) {
            sessionsClosed.set(true);

            log.trace("ScannerManager asked to close all tracked scanners");
            super.close();

            for (ScannerSession scanner : new HashSet<>(sessionInstances)) {
                close(scanner);
            }
        }
    }

}
