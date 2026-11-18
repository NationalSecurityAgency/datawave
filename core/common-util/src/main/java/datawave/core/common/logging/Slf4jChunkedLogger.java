package datawave.core.common.logging;

import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * A {@link ChunkedLogger} implementation that wraps around an SLF4J {@link Logger} with a designated log level.
 */
public class Slf4jChunkedLogger extends ChunkedLogger {

    private final Logger log;
    private final Level level;

    public Slf4jChunkedLogger(Logger log, Level level, String chunkPartPrefix, int maxChars) {
        super(chunkPartPrefix, maxChars);
        this.log = log;
        this.level = level;
    }

    @Override
    public boolean isLogLevelEnabled() {
        return log.isEnabledForLevel(level);
    }

    @Override
    public void log(String message) {
        log.atLevel(level).log(message);
    }
}
