/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

import com.google.common.collect.Ordering;

/**
 * Normally distributes data based on a generic slice object.
 */
public abstract class AbstractSliceDistributor<S extends Comparable<S>> implements FlagDistributor {

    protected TreeMap<S,Set<InputFile>> buckets;
    protected FlagDataTypeConfig fc;

    @Override
    public void setup(FlagDataTypeConfig fc) {
        this.fc = fc;
        buckets = new TreeMap<>(getComparator(fc.isLifo()));
    }

    public Comparator<S> getComparator(boolean lifo) {
        if (lifo) {
            return (o1, o2) -> {
                // return the reverse order
                return Ordering.natural().compare(o2, o1);
            };
        } else {
            return Ordering.natural();
        }
    }

    @Override
    public final boolean hasNext(boolean mustHaveMax) {
        int max = mustHaveMax ? fc.getMaxFlags() : 1;
        int total = 0;
        for (Set<InputFile> bucket : buckets.values()) {
            for (InputFile inputFile : bucket) {
                total += inputFile.getMaps();
            }
            if (total >= max)
                break;
        }
        return total >= max;
    }

    @Override
    public final Collection<InputFile> next(SizeValidator validator) {
        TreeMap<S,Integer> stats = new TreeMap<>(getComparator(fc.isLifo()));
        int totalMaps = 0;
        for (Map.Entry<S,Set<InputFile>> pair : buckets.entrySet()) {
            int typeMaps = 0;
            for (InputFile inFile : pair.getValue()) {
                typeMaps += inFile.getMaps();
            }
            if (typeMaps == 0)
                continue;
            totalMaps += typeMaps;
            stats.put(pair.getKey(), typeMaps);
        }

        if (totalMaps == 0)
            return Collections.EMPTY_SET;
        HashSet<InputFile> flagFiles = new HashSet<>();
        if (totalMaps < fc.getMaxFlags()) {
            // get everything
            Iterator<Map.Entry<S,Set<InputFile>>> it = buckets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<S,Set<InputFile>> pair = it.next();
                flagFiles.addAll(pair.getValue());
                it.remove();
            }
        } else {
            // get uniform distribution
            for (Map.Entry<S,Integer> stat : stats.entrySet()) {
                double slicemaps = stat.getValue().doubleValue();
                int maps = (int) Math.ceil(slicemaps / (double) totalMaps * (double) fc.getMaxFlags());
                Iterator<InputFile> pendingFiles = buckets.get(stat.getKey()).iterator();
                while (pendingFiles.hasNext() && maps > 0) {
                    InputFile flag = pendingFiles.next();
                    maps -= flag.getMaps();
                    flagFiles.add(flag);
                    pendingFiles.remove();
                }
            }
        }
        return flagFiles;
    }

}
