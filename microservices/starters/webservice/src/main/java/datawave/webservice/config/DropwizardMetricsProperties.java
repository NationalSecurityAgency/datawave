package datawave.webservice.config;

import java.util.concurrent.TimeUnit;

public class DropwizardMetricsProperties {
    private String reportHost;
    private int reportPort;
    private String reporterClass;
    private int reportInterval;
    private TimeUnit reportIntervalTimeUnit;

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

    public String getReporterClass() {
        return reporterClass;
    }

    public void setReporterClass(String reporterClass) {
        this.reporterClass = reporterClass;
    }

    public int getReportInterval() {
        return reportInterval;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public TimeUnit getReportIntervalTimeUnit() {
        return reportIntervalTimeUnit;
    }

    public void setReportIntervalTimeUnit(TimeUnit reportIntervalTimeUnit) {
        this.reportIntervalTimeUnit = reportIntervalTimeUnit;
    }
}
