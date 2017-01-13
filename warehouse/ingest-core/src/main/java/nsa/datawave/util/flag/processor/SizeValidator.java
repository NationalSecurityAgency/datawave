package nsa.datawave.util.flag.processor;

import java.util.Collection;

import nsa.datawave.util.flag.InputFile;
import nsa.datawave.util.flag.config.FlagDataTypeConfig;

/**
 * An interface for a class that will validate the size of a set of input files for use in a flag file.
 */
public interface SizeValidator {
    public boolean isValidSize(FlagDataTypeConfig fc, Collection<InputFile> files);
}
