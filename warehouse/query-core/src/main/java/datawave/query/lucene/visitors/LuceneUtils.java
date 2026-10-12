package datawave.query.lucene.visitors;

import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;

/**
 * Utility methods for LUCENE queries/nodes.
 */
public final class LuceneUtils {

    private static final EscapeQuerySyntax escapedSyntax = new EscapeQuerySyntaxImpl();

    /**
     * Return the escaped form of a non-quoted sequence using the locale {@link Locale#getDefault()}.
     *
     * @param sequence
     *            the sequence to escape
     * @return the escaped form of the sequence
     */
    public static CharSequence escape(CharSequence sequence) {
        return escape(sequence, Locale.getDefault());
    }

    /**
     * Return the escaped form of a non-quoted sequence using the given {@link Locale}.
     *
     * @param sequence
     *            the sequence to escape
     * @param locale
     *            the locale to use when escaping
     * @return the escaped form of the sequence
     */
    public static CharSequence escape(CharSequence sequence, Locale locale) {
        return escapedSyntax.escape(sequence, locale, EscapeQuerySyntax.Type.NORMAL);
    }

    /**
     * Return the escaped form of a quoted sequence using the locale {@link Locale#getDefault()}.
     *
     * @param sequence
     *            the sequence to escape
     * @return the escaped form of the sequence
     */
    public static CharSequence escapeQuoted(CharSequence sequence) {
        return escapeQuoted(sequence, Locale.getDefault());
    }

    /**
     * Return the escaped form of a quoted sequence using the given {@link Locale}.
     *
     * @param sequence
     *            the sequence to escape
     * @param locale
     *            the locale to use when escaping
     * @return the escaped form of the sequence
     */
    public static CharSequence escapeQuoted(CharSequence sequence, Locale locale) {
        return escapedSyntax.escape(sequence, locale, EscapeQuerySyntax.Type.STRING);
    }

    private LuceneUtils() {
        // Do not allow this class to be instantiated.
        throw new UnsupportedOperationException();
    }
}
