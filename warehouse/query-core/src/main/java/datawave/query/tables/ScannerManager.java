package datawave.query.tables;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.ScannerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages instances of both {@link ScannerSession} and {@link ScannerBase}.
 */
public class ScannerManager {

    private static final Logger log = LoggerFactory.getLogger(ScannerManager.class);

    protected final Set<ScannerBase> baseInstances = Collections.synchronizedSet(new HashSet<>());
    protected final Set<ScannerSession> sessionInstances = Collections.synchronizedSet(new HashSet<>());

    public ScannerManager() {
        // empty constructor
    }

    /**
     * Adds a {@link ScannerBase}
     *
     * @param scanner
     *            a ScannerBase instance
     */
    public void addScanner(ScannerBase scanner) {
        log.trace("Adding scanner {}", scanner);
        this.baseInstances.add(scanner);
    }

    /**
     * Adds a {@link ScannerSession}
     *
     * @param scanner
     *            a ScannerSession instance
     */
    public void addScanner(ScannerSession scanner) {
        log.trace("Adding scanner {}", scanner);
        this.sessionInstances.add(scanner);
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
            log.debug("Closed scanner {}", scanner);
        } else {
            log.warn("ScannerManager was asked to close untracked scanner base: {}", scanner);
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
     * Closes all scanners tracked by the {@link ScannerManager}
     */
    public void close() {
        log.trace("ScannerManager asked to close all tracked scanners");

        for (ScannerBase scanner : new HashSet<>(baseInstances)) {
            close(scanner);
        }

        for (ScannerSession scanner : new HashSet<>(sessionInstances)) {
            close(scanner);
        }
    }

}
