package datawave.ingest.util.cache.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import datawave.ingest.util.cache.delete.mode.DeleteJobCacheMode;

import java.util.Arrays;
import java.util.Optional;

/** JCommander converter class to convert a command line string to the DeleteJobCacheMode object */
public class DeleteModeConverter implements IStringConverter<DeleteJobCacheMode> {
    @Override
    public DeleteJobCacheMode convert(String value) {
        Optional<DeleteJobCacheMode> mode = DeleteJobCacheMode.getDeleteCacheMode(DeleteJobCacheMode.Mode.valueOf(value));
        if (!mode.isPresent()) {
            throw new ParameterException(value + " is not a valid mode. " + Arrays.toString(DeleteJobCacheMode.Mode.values()));
        }
        return mode.get();
    }
}
