package datawave.metrics;

public class MetricsCDIProperties {

    private String reportHost = "localhost";

    private int reportPort = 54321;

    private int reportInterval = 60;

    private String reportIntervalTimeUnit = "SECONDS";

    private String reporterClass = "datawave.metrics.TimelyMetricsReporterFactory";

    public String getReportHost() {
        return reportHost;
    }

    public void setReportHost(String reportHost) {
        this.reportHost = reportHost;
    }

    public int getReportPort() {
        return reportPort;
    }

    public void setReportPort(int reportPort) {
        this.reportPort = reportPort;
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

    public String getReporterClass() {
        return reporterClass;
    }

    public void setReporterClass(String reporterClass) {
        this.reporterClass = reporterClass;
    }
}
