package datawave.core.common.logging;

/**
 * A log wrapper designed to log buffered messages in chunked parts. Data is appended to the buffer via {@link #write(String)} if the log level is enabled as
 * indicated by {@link #isLogLevelEnabled()}. If the buffer length exceeds the configured maximum characters, the buffer is flushed to the log and then cleared.
 * If a chunk prefix is configured, the flushed buffer will always be preceded with a line containing the chunk prefix and an indication of the chunk number.
 */
public abstract class ChunkedLogger {

    protected final StringBuilder buffer = new StringBuilder();
    protected final String chunkPrefix;
    protected final int maxChars;
    protected int chunkTotal = 1;

    protected ChunkedLogger(String chunkPrefix, int maxChars) {
        this.chunkPrefix = chunkPrefix;
        this.maxChars = maxChars;
    }

    /**
     * Return whether the log level is enabled.
     *
     * @return true if the log level is enabled, or false otherwise
     */
    public abstract boolean isLogLevelEnabled();

    /**
     * Log the given message to the underlying logger.
     *
     * @param message
     *            the message
     */
    public abstract void log(String message);

    /**
     * Append the given line to the buffer if the log level is enabled. If the buffer length is greater than the configured max characters, the buffer contents
     * will be flushed to the log and cleared.
     *
     * @param line
     *            the line to write
     * @see #flush()
     */
    public void write(String line) {
        if (isLogLevelEnabled()) {
            buffer.append(line);
            if (buffer.length() >= maxChars) {
                flush();
            }
        }
    }

    /**
     * Flush the contents of the buffer to the log if the log level is enabled. If a chunk prefix was configured, the log message will be preceded by a line
     * with the format {@code <chunkPrefix> [part <chunkTotal>]}.
     */
    public void flush() {
        if (isLogLevelEnabled() && buffer.length() > 0) {
            // Construct a chunk prefix only if one was initially provided. Otherwise, the contents of the buffer should be written as is.
            String prefix = chunkPrefix != null ? chunkPrefix + " [part " + chunkTotal + "]\n" : "";
            chunkTotal++;
            log(prefix + buffer);
            buffer.setLength(0);
        }
    }
}
