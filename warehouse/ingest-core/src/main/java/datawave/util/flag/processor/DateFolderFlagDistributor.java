/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag.processor;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * This will create a bucket per group, per datatype (i.e. a folder to search as defined in the config). Within a bucket the files will be sorted by file date
 * in lifo or fifo order as configured.
 */
public class DateFolderFlagDistributor extends AbstractSliceDistributor<DateFolderFlagDistributor.DFKey> {

    String grouping;
    DateUtils util = new DateUtils();
    List<String> folders;

    @Override
    public void setup(FlagDataTypeConfig fdtc) {
        super.setup(fdtc);
        grouping = fdtc.getDistributionArgs();
        folders = fdtc.getFolder();
        if (grouping == null || !DateUtils.GROUPS.contains(grouping.toLowerCase())) {
            throw new IllegalArgumentException("Grouping must be none|year|month|day");
        }
        if (fc.getMaxFlags() < 1)
            throw new IllegalArgumentException("Invalid number of flags for this datatype provided (" + fc.getMaxFlags() + ")");
        buckets.clear();
    }

    @Override
    public boolean addInputFile(InputFile inputFile) throws UnusableFileException {
        String path = inputFile.getDirectory();
        long slice = util.getBucket(grouping, path);
        // we should never use default...
        String folder = "default";
        for (String string : folders) {
            if (path.contains(string)) {
                folder = string;
                break;
            }
        }
        DFKey bucket = new DFKey(slice, folder);
        Set<InputFile> bucketList = buckets.get(bucket);
        if (bucketList == null) {
            bucketList = new TreeSet<>(fc.isLifo() ? InputFile.LIFO : InputFile.FIFO);
            buckets.put(bucket, bucketList);
        }
        return bucketList.add(inputFile);
    }

    protected class DFKey implements Comparable<DFKey> {

        long group;
        String folder;

        public DFKey(long group, String folder) {
            this.group = group;
            this.folder = folder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final DFKey other = (DFKey) obj;
            if (this.group != other.group) {
                return false;
            }
            if ((this.folder == null) ? (other.folder != null) : !this.folder.equals(other.folder)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + (int) (this.group ^ (this.group >>> 32));
            hash = 97 * hash + (this.folder != null ? this.folder.hashCode() : 0);
            return hash;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        @Override
        public int compareTo(DFKey o) {
            // sort on group, then by folder
            int comparison = 0;
            if (this.group < o.group) {
                comparison = -1;
            } else if (this.group > o.group) {
                comparison = 1;
            }
            if (comparison == 0) {
                comparison = this.folder.compareTo(o.folder);
            }
            return comparison;
        }
    }
}
