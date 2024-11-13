/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag.processor;

import java.util.Set;
import java.util.TreeSet;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * This will create a bucket per group, independent of the datatype. Within a bucket the files will be sorted by file date in lifo or fifo order as configured.
 *
 */
public class DateFlagDistributor extends AbstractSliceDistributor<Long> {

    private String grouping;
    private DateUtils util = new DateUtils();

    @Override
    public void setup(FlagDataTypeConfig fmc) {
        super.setup(fmc);
        grouping = fmc.getDistributionArgs();
        if (grouping == null || !DateUtils.GROUPS.contains(grouping.toLowerCase())) {
            throw new IllegalArgumentException("Grouping must be none|year|month|day");
        }
        if (fc.getMaxFlags() < 1)
            throw new IllegalArgumentException("Invalid number of flags for this datatype provided (" + fc.getMaxFlags() + ")");
        buckets.clear();
    }

    @Override
    public boolean addInputFile(InputFile inputFile) throws UnusableFileException {
        long bucket = util.getBucket(grouping, inputFile.getDirectory());
        Set<InputFile> bucketList = buckets.get(bucket);
        if (bucketList == null) {
            bucketList = new TreeSet<>(fc.isLifo() ? InputFile.LIFO : InputFile.FIFO);
            buckets.put(bucket, bucketList);
        }
        return bucketList.add(inputFile);
    }

}
