package datawave.helpers;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.event.Level;

public interface Output {

    void write(String line);

    default void writeln(String line) {
        write("\n" + line);
    }

    void flush();

    class Slf4jOutput implements Output {

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
