package datawave.ingest.util.cache.converter;

import com.beust.jcommander.IStringConverter;
import datawave.ingest.util.cache.lease.JobCacheLockFactory;

/** JCommander converter class to convert a command line string to the JobCacheLockFactory object */
public class CacheLockConverter implements IStringConverter<JobCacheLockFactory> {
    @Override
    public JobCacheLockFactory convert(String value) {
        return JobCacheLockFactory.getLockFactory(value);
    }
}
