package datawave.ingest.util.cache.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import datawave.ingest.util.cache.lease.JobCacheLockFactory;

import java.util.Arrays;
import java.util.Optional;

/** JCommander converter class to convert a command line string to the JobCacheLockFactory object */
public class CacheLockConverter implements IStringConverter<JobCacheLockFactory> {
    @Override
    public JobCacheLockFactory convert(String value) {
        Optional<JobCacheLockFactory> mode = JobCacheLockFactory.getLockFactory(JobCacheLockFactory.Mode.valueOf(value));
        if (!mode.isPresent()) {
            throw new ParameterException(value + " is not a valid mode. " + Arrays.toString(JobCacheLockFactory.Mode.values()));
        }
        return mode.get();
    }
}
