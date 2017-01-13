package nsa.datawave.metrics.web.stats.queries;

/**
 * Class to encapsulate the server query rate.
 * 
 */
public class ServerQueryRate implements Comparable<ServerQueryRate> {
    
    protected String tabletServer;
    
    protected long rate;
    
    public ServerQueryRate(String tserver, long lRate) {
        tabletServer = tserver;
        rate = lRate;
    }
    
    public ServerQueryRate(String tserver, double dRate) {
        tabletServer = tserver;
        rate = (long) (dRate + 0.5);
    }
    
    @Override
    public int compareTo(ServerQueryRate o) {
        return Long.compare(rate, o.rate);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ServerQueryRate)) {
            return false;
        }
        return 0 == compareTo((ServerQueryRate) obj);
    }
    
    @Override
    public int hashCode() {
        return Long.valueOf(this.rate).hashCode();
    }
}
