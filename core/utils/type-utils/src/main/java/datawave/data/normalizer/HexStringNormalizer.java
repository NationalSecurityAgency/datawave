package datawave.data.normalizer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HexStringNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = -2056362158103923525L;
    private static final Logger log = LoggerFactory.getLogger(HexStringNormalizer.class);
    private final Pattern pattern;
    
    public HexStringNormalizer() {
        this("(0x)?([0-9a-fA-F]+)");
    }
    
    protected HexStringNormalizer(String regex) {
        pattern = Pattern.compile(regex);
    }
    
    protected String getNormalizedHex(String hex) {
        if (hex.length() % 2 == 0) {
            return LC_NO_DIACRITICS_NORMALIZER.normalize(hex);
        }
        
        StringBuilder buf = new StringBuilder(hex.length() + 1);
        return LC_NO_DIACRITICS_NORMALIZER.normalize(buf.append("0").append(hex).toString());
    }
    
    protected Matcher validate(String fieldValue) {
        if (StringUtils.isEmpty(fieldValue)) {
            logAndThrow("Field may not be null or empty.");
        }
        
        Matcher matcher = pattern.matcher(fieldValue);
        if (!matcher.matches()) {
            logAndThrow(String.format("Failed to normalize hex value : %s.", fieldValue));
        }
        
        return matcher;
    }
    
    @Override
    public String normalize(String fieldValue) {
        Matcher matcher = validate(fieldValue);
        
        return getNormalizedHex(matcher.group(2));
    }
    
    private void logAndThrow(String msg) {
        if (log.isDebugEnabled()) {
            log.debug(msg);
        }
        throw new IllegalArgumentException(msg);
    }
    
    @Override
    public String normalizeRegex(String fieldRegex) {
        return normalize(fieldRegex);
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
