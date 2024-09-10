package datawave.webservice.query.database;

public class CachedResultsCleanupConfiguration {

    private int daysToLive = 1;

    public int getDaysToLive() {
        return daysToLive;
    }

    public void setDaysToLive(int daysToLive) {
        this.daysToLive = daysToLive;
    }

}
