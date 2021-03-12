package datawave.ingest.util.cache.load.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import org.apache.hadoop.fs.Path;

/**
 * JCommander converter class to convert a command line string to the Path object
 */
public class HadoopPathConverter implements IStringConverter<Path> {
    @Override
    public Path convert(String value) {
        try {
            return new Path(value);
        } catch (IllegalArgumentException iae) {
            throw new ParameterException(value + " could not be converted to a Path ", iae);
        }
    }
}
