package datawave.mr.bulk.split;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;

public class RfileSplit {
    public Path path;
    public int start;
    public long length;
    public String[] hosts;
    private int hashCode;

    public RfileSplit(Path path, int start, long length, String[] locations) {
        this.path = path;
        this.start = start;
        this.length = length;
        this.hosts = locations;
        hashCode = new HashCodeBuilder().append(path).append(start).append(length).append(hosts).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RfileSplit) {
            return new EqualsBuilder().append(path, ((RfileSplit) obj).path).append(start, ((RfileSplit) obj).start).append(length, ((RfileSplit) obj).length)
                            .append(hosts, ((RfileSplit) obj).hosts).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
