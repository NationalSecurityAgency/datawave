package datawave.webservice.common.remote;

import java.util.Collections;
import java.util.List;

import com.codahale.metrics.Counter;

public class RemoteHttpServiceConfiguration {
    private boolean useSrvDNS = false;

    private List<String> srvDnsServers = Collections.singletonList("127.0.0.1");

    private int srvDnsPort = 8600;

    private String serviceScheme = "https";

    private String serviceHost = "localhost";

    private int servicePort = 8443;

    private String serviceURI = "/";

    private int maxConnections = 100;

    private int retryCount = 5;

    private int unavailableRetryCount = 15;

    private int unavailableRetryDelay = 2000;

    private Counter retryCounter = new Counter();

    private Counter failureCounter = new Counter();

    public void setUseSrvDNS(boolean useSrvDNS) {
        this.useSrvDNS = useSrvDNS;
    }

    public void setSrvDnsServers(List<String> srvDnsServers) {
        this.srvDnsServers = srvDnsServers;
    }

    public void setSrvDnsPort(int srvDnsPort) {
        this.srvDnsPort = srvDnsPort;
    }

    public void setServiceScheme(String serviceScheme) {
        this.serviceScheme = serviceScheme;
    }

    public void setServiceHost(String serviceHost) {
        this.serviceHost = serviceHost;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public void setServiceURI(String serviceURI) {
        this.serviceURI = serviceURI;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void setUnavailableRetryCount(int unavailableRetryCount) {
        this.unavailableRetryCount = unavailableRetryCount;
    }

    public void setUnavailableRetryDelay(int unavailableRetryDelay) {
        this.unavailableRetryDelay = unavailableRetryDelay;
    }

    public boolean isUseSrvDNS() {
        return useSrvDNS;
    }

    public List<String> getSrvDnsServers() {
        return srvDnsServers;
    }

    public int getSrvDnsPort() {
        return srvDnsPort;
    }

    public String getServiceScheme() {
        return serviceScheme;
    }

    public String getServiceHost() {
        return serviceHost;
    }

    public int getServicePort() {
        return servicePort;
    }

    public String getServiceURI() {
        return serviceURI;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getUnavailableRetryCount() {
        return unavailableRetryCount;
    }

    public int getUnavailableRetryDelay() {
        return unavailableRetryDelay;
    }

    public Counter getRetryCounter() {
        return retryCounter;
    }

    public void setRetryCounter(Counter retryCounter) {
        this.retryCounter = retryCounter;
    }

    public Counter getFailureCounter() {
        return failureCounter;
    }

    public void setFailureCounter(Counter failureCounter) {
        this.failureCounter = failureCounter;
    }
}
