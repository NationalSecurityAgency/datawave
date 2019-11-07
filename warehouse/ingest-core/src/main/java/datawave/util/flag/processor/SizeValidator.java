package datawave.util.flag.processor;

import java.util.Collection;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * An interface for a class that will validate the size of a set of input files for use in a flag file.
 */
public interface SizeValidator {
    boolean isValidSize(FlagDataTypeConfig fc, Collection<InputFile> files);
}
