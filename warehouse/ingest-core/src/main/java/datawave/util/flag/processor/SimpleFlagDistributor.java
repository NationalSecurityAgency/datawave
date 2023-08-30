/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * No groupings, just returns files that are pending in no specific order
 */
public class SimpleFlagDistributor implements FlagDistributor {

    private static final Logger log = LoggerFactory.getLogger(SimpleFlagDistributor.class);

    private Set<InputFile> inputs;
    private FlagDataTypeConfig fc;

    @Override
    public void setup(FlagDataTypeConfig fc) {
        this.fc = fc;
        inputs = new TreeSet<>(fc.isLifo() ? InputFile.LIFO : InputFile.FIFO);
    }

    @Override
    public boolean addInputFile(InputFile inputFile) {
        return inputs.add(inputFile);
    }

    @Override
    public boolean hasNext(boolean mustHaveMax) {
        // if there's nothing to do, then reset the last time again
        return mustHaveMax ? inputs.size() >= fc.getMaxFlags() : !inputs.isEmpty();
    }

    @Override
    public Collection<InputFile> next(SizeValidator validator) {
        int size = inputs.size();
        if (size == 0)
            return Collections.EMPTY_SET;
        Collection<InputFile> list = new HashSet<>();
        if (size < fc.getMaxFlags()) {
            list.addAll(inputs);
            inputs.clear();
        } else {
            int count = 0;
            Iterator<InputFile> it = inputs.iterator();
            // while we have more potential files, and we have potentially room to add one
            while (it.hasNext() && (count < fc.getMaxFlags())) {
                InputFile inFile = it.next();

                int maps = inFile.getMaps();
                if (maps > fc.getMaxFlags()) {
                    log.warn("Estimated map count ({}) for file exceeds maxFlags ({}). Consider increasing maxFlags to accommodate larger files, or split this file into smaller chunks. File: {}",
                                    maps, fc.getMaxFlags(), inFile.getFileName());
                }

                // update the count, and break out if this file would pass our threshold
                count += maps;
                if (count > fc.getMaxFlags()) {
                    break;
                }

                // add it to the list
                list.add(inFile);

                // if valid (or only one file in the list), then continue normally
                if (validator.isValidSize(fc, list) || (list.size() == 1)) {
                    it.remove();
                } else {
                    // else remove the file back out and abort
                    list.remove(inFile);
                    break;
                }
            }
        }
        return list;
    }

}
