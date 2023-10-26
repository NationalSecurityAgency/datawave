/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag.processor;

import java.io.IOException;
import java.util.Collection;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * Allows different implementation of flag file distributions for job queuing.
 */
public interface FlagDistributor {
    /**
     * Configures FlagDistributor for a particular datatype. Resets any saved state within this distributor.
     *
     * @param flagDataTypeConfig
     *            configuration for this flag file datatype
     */
    void loadFiles(FlagDataTypeConfig flagDataTypeConfig) throws IOException;

    /**
     * Determines if a flag file should be created. When called with true, the number of queued files should be greater than or equal to the number of maximum
     * flags for this datatype. When called with false, there should be at least one file.
     *
     * @param mustHaveMax
     *            true for a full job, false for the existence of any files
     * @return true if there are sufficient files/maps to create a flag file
     * @throws IllegalStateException if loadFiles was not called first
     */
    boolean hasNext(boolean mustHaveMax);

    /**
     * Returns a list of InputFiles to be included in a flag file. It is intended that this method be called in conjunction with hasNext, but that is not
     * required. Behavior is not guaranteed when called w/out using hasNext.
     *
     * @param validator
     *            An object that should be used to ensure the returned list will produce a valid flag file in terms of size
     * @return The list of files to be put into a flag file
     * @throws IllegalStateException if loadFiles was not called first
     */
    Collection<InputFile> next(SizeValidator validator);

}
