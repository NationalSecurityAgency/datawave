package datawave.ingest.util.cache.converter;

import com.beust.jcommander.IStringConverter;

import java.util.regex.Pattern;

/**
 * JCommander converter class to convert a command line string to the Path object
 */
public class RegexPatternConverter implements IStringConverter<Pattern> {
    @Override
    public Pattern convert(String value) {
        return Pattern.compile(value);
    }
}
