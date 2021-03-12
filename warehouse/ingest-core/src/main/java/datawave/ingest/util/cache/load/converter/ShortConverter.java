package datawave.ingest.util.cache.load.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

/** JCommander converter class to convert a command line string to the Short object */
public class ShortConverter implements IStringConverter<Short> {
    @Override
    public Short convert(String value) {
        try {
            return Short.parseShort(value);
        } catch (NumberFormatException nfe) {
            throw new ParameterException(value + " could not be converted to a short ", nfe);
        }
    }
}
