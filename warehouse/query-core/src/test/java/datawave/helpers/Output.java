package datawave.helpers;

/**
 * A simple output interface.
 */
public interface Output {

    /**
     * Write the given line to the output.
     *
     * @param line
     *            the line to write
     */
    void write(String line);

    /**
     * Write the given line prefixed with a {@code \n} to the output.
     *
     * @param line
     *            the line to write
     */
    default void writeln(String line) {
        write("\n" + line);
    }

    /**
     * Flush the output.
     */
    void flush();
}
