package datawave.microservice.common.storage;


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class QueryStats {
    private int numResults;

    public int getNumResults() {
        return numResults;
    }

    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(numResults).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryStats) {
            QueryStats other = (QueryStats)obj;
            return new EqualsBuilder().append(numResults, other.numResults).build();
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("numResults", numResults).build();
    }
}
