package datawave.data.normalizer;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;

/**
 * A Normalizer which performs the following steps:
 * <ol>
 * <li>Unicode canonical decomposition ({@link Form#NFD})</li>
 * <li>Removal of diacritical marks</li>
 * <li>Unicode canonical composition ({@link Form#NFC})</li>
 * <li>lower casing in the {@link Locale#ENGLISH English local}
 * </ol>
 */
public class LcNoDiacriticsNormalizer extends AbstractNormalizer<String> {
    private static final long serialVersionUID = -7922074256473963293L;
    private static final Pattern diacriticals = Pattern.compile("\\p{InCombiningDiacriticalMarks}");
    
    public String normalize(String fieldValue) {
        if (null == fieldValue) {
            return null;
        }
        String decomposed = Normalizer.normalize(fieldValue, Form.NFD);
        String noDiacriticals = removeDiacriticalMarks(decomposed);
        String recomposed = Normalizer.normalize(noDiacriticals, Form.NFC);
        return recomposed.toLowerCase(Locale.ENGLISH);
    }
    
    private String removeDiacriticalMarks(String str) {
        Matcher matcher = diacriticals.matcher(str);
        return matcher.replaceAll("");
    }
    
    public String normalizeRegex(String fieldRegex) {
        if (null == fieldRegex) {
            return null;
        }
        String decomposed = Normalizer.normalize(fieldRegex, Form.NFD);
        String noDiacriticals = removeDiacriticalMarks(decomposed);
        String recomposed = Normalizer.normalize(noDiacriticals, Form.NFC);
        try {
            JavaRegexAnalyzer regex = new JavaRegexAnalyzer(recomposed);
            regex.applyRegexCaseSensitivity(false);
            return regex.getRegex();
        } catch (JavaRegexParseException e) {
            throw new IllegalArgumentException("Unable to parse regex " + fieldRegex, e);
        }
    }
    
    @Override
    public boolean normalizedRegexIsLossy(String regex) {
        // Despite this normalizer actually being lossy, we are still
        // returning false as users are used to overmatching when including
        // diacritics or upper case letter. We may consider changing this
        // down the road, but for now returning false.
        return false;
    }
    
    @Override
    public String normalizeDelegateType(String delegateIn) {
        return normalize(delegateIn);
    }
    
    @Override
    public String denormalize(String in) {
        return in;
    }
}
