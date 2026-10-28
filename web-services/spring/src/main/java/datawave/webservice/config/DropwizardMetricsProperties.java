package datawave.webservice.config;

public class DropwizardMetricsProperties {

    private String host = "localhost";

    private int port = 54321;

    private int reportInterval = 60;

    private String reportIntervalTimeUnit = "SECONDS";

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getReportInterval() {
        return reportInterval;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public String getReportIntervalTimeUnit() {
        return reportIntervalTimeUnit;
    }

    public void setReportIntervalTimeUnit(String reportIntervalTimeUnit) {
        this.reportIntervalTimeUnit = reportIntervalTimeUnit;
    }
}
