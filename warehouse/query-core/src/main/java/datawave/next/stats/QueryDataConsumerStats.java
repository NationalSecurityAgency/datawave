package datawave.next.stats;

import java.io.Serializable;

public class QueryDataConsumerStats implements Serializable {

    private static final long serialVersionUID = 4859710545760282597L;

    private int queryDataSeen = 0;
    private int nullDataSeen = 0;
    private int numShardScans = 0;
    private int numDocScans = 0;

    public void incrementQueryDataSeen() {
        queryDataSeen++;
    }

    public void incrementNullDataSeen() {
        nullDataSeen++;
    }

    public void incrementNumShardScans() {
        numShardScans++;
    }

    public void incrementNumDocScans() {
        numDocScans++;
    }

    public int getQueryDataSeen() {
        return queryDataSeen;
    }

    public int getNullDataSeen() {
        return nullDataSeen;
    }

    public int getNumShardScans() {
        return numShardScans;
    }

    public int getNumDocScans() {
        return numDocScans;
    }
}
