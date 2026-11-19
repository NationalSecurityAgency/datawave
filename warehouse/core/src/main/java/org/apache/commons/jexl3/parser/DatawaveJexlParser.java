package org.apache.commons.jexl3.parser;

import java.io.IOException;

import org.apache.commons.jexl3.JexlBuilder;

/**
 * Creates JEXL 3.5 parsers with the identifier vocabulary used by DataWave with JEXL 3.3.
 */
public final class DatawaveJexlParser {

    private DatawaveJexlParser() {}

    public static Parser create() {
        SimpleCharStream stream = new QueryCharStream();
        Parser parser = new Parser(new ParserTokenManager(stream));
        parser.jj_input_stream = stream;
        parser.jjtree = new DatawaveParserState();
        // These newly reserved words were valid field, variable, and namespace names in JEXL 3.3.
        // Keep try, catch, finally, throw, and instanceof as identifiers.
        parser.token_source.jexl331 = false;
        return parser;
    }

    public static JexlBuilder configure(JexlBuilder builder) {
        if (builder.parserFactory() == null) {
            builder.parserFactory(DatawaveJexlParser::create);
        }
        return builder;
    }

    private static final class QueryCharStream extends SimpleCharStream {
        private QueryCharStream() {
            super(new StringProvider(";"), 1, 1);
        }

        @Override
        protected int streamRead(char[] buffer, int offset, int length) throws IOException {
            int count = super.streamRead(buffer, offset, length);
            if (count == -1) {
                // The generated lexer catches IOException as its normal EOF signal. Avoid constructing a stack trace
                // for every lookahead past the end of a query. AbstractCharStream still performs its usual buffer cleanup.
                streamClose();
                throw new EndOfInputException();
            }
            return count;
        }
    }

    private static final class EndOfInputException extends IOException {
        private static final long serialVersionUID = 1L;

        private EndOfInputException() {
            super("PGCC end of stream");
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}
