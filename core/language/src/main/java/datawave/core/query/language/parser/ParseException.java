package datawave.core.query.language.parser;

public class ParseException extends Exception {
    private static final long serialVersionUID = 1L;

    public ParseException(String s) {
        super(s);
    }

    public ParseException(String s, Throwable t) {
        super(s, t);
    }

    public ParseException(Throwable t) {
        super(t);
    }
}
