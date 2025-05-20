/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag.processor;

import java.util.Collection;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * Allows different implementation of flag file distributions for job queuing.
 */
public interface FlagDistributor {
    /**
     * Allows for the setup/configuration of the FlagDistributor. Anything that needs to be configured for a particular data type must be contained within the
     * FlagDataTypeConfig. This call to this method should reset any saved state within this distributor.
     *
     * @param fdtc
     *            a flag data type config
     */
    void setup(FlagDataTypeConfig fdtc);

    /**
     * When pending files are discovered, they should be queued
     *
     * @param inputFile
     *            an input file
     * @return true/false based on if files are added
     * @throws UnusableFileException
     *             if the file is unusable
     */
    boolean addInputFile(InputFile inputFile) throws UnusableFileException;

    /**
     * Determines if a flag file should be created. When called with true, the number of queued files should be greater than or equal to the number of maximum
     * flags for this datatype. When called with false, there should be at least one file.
     *
     * @param mustHaveMax
     *            true for a full job, false for the existence of any files
     * @return true if there are sufficient files/maps to create a flag file
     */
    boolean hasNext(boolean mustHaveMax);

    /**
     * Returns a list of InputFiles to be included in a flag file. It is intended that this method be called in conjunction with hasNext, but that is not
     * required. Behavior is not guaranteed when called w/out using hasNext.
     *
     * @param validator
     *            An object that should be used to ensure the returned list will produce a valid flag file in terms of size
     * @return The list of files to be put into a flag file
     */
    Collection<InputFile> next(SizeValidator validator);

}
