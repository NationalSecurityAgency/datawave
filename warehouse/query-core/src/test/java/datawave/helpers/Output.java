package datawave.helpers;

import java.io.PrintStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public interface Output {

    void write(String line);

    default void writeln(String line) {
        write("\n" + line);
    }

    void flush();

    class ApacheLog4JOutput implements Output {

        private final StringBuilder sb = new StringBuilder();
        private final Logger log;
        private final Level level;

        public static ApacheLog4JOutput info(Logger log) {
            return new ApacheLog4JOutput(log, Level.INFO);
        }

        public static ApacheLog4JOutput debug(Logger log) {
            return new ApacheLog4JOutput(log, Level.DEBUG);
        }

        public static ApacheLog4JOutput trace(Logger log) {
            return new ApacheLog4JOutput(log, Level.TRACE);
        }

        public ApacheLog4JOutput(Logger log, Level level) {
            this.log = log;
            this.level = level;
        }

        @Override
        public void write(String line) {
            sb.append(line);
        }

        @Override
        public void flush() {
            log.log(level, sb.toString());
            sb.setLength(0);
        }
    }

    class PrintStreamOutput implements Output {

        private final StringBuilder sb = new StringBuilder();
        private final PrintStream out;

        public static PrintStreamOutput of(PrintStream out) {
            return new PrintStreamOutput(out);
        }

        public PrintStreamOutput(PrintStream out) {
            this.out = out;
        }

        @Override
        public void write(String line) {
            sb.append(line);
        }

        @Override
        public void flush() {
            out.print(sb);
            sb.setLength(0);
        }
    }
}
