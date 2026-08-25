package datawave.helpers;

import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * A {@link Output} implementation that wraps around a {@link Logger} with a designated log level.
 */
public class Slf4jOutput implements Output {

    private final StringBuilder sb = new StringBuilder();
    private final Logger log;
    private final Level level;

    public static Slf4jOutput info(Logger log) {
        return new Slf4jOutput(log, Level.INFO);
    }

    public static Slf4jOutput debug(Logger log) {
        return new Slf4jOutput(log, Level.DEBUG);
    }

    public static Slf4jOutput trace(Logger log) {
        return new Slf4jOutput(log, Level.TRACE);
    }

    public Slf4jOutput(Logger log, Level level) {
        this.log = log;
        this.level = level;
    }

    @Override
    public void write(String line) {
        sb.append(line);
    }

    @Override
    public void flush() {
        log.atLevel(level).log(sb.toString());
        sb.setLength(0);
    }
}
