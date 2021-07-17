package datawave.ingest.util.cache.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import datawave.ingest.util.cache.load.mode.LoadJobCacheMode;

import java.util.Arrays;
import java.util.Optional;

/** JCommander converter class to convert a command line string to the LoadJobCacheMode object */
public class LoadModeConverter implements IStringConverter<LoadJobCacheMode> {
    @Override
    public LoadJobCacheMode convert(String value) {
        Optional<LoadJobCacheMode> mode = LoadJobCacheMode.getLoadCacheMode(LoadJobCacheMode.Mode.valueOf(value));
        if (!mode.isPresent()) {
            throw new ParameterException(value + " is not a valid mode. " + Arrays.toString(LoadJobCacheMode.Mode.values()));
        }
        return mode.get();
    }
}
