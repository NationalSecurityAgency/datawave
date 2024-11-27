package datawave.query.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.util.sortedset.FileSortedSet;

public class IvaratorConfig implements Serializable {

    private List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs = Collections.emptyList();
    private String ivaratorFstHdfsBaseURIs = null;
    private int ivaratorCacheBufferSize = 10000;
    private long ivaratorCacheScanPersistThreshold = 100000L;
    private long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    private int maxFieldIndexRangeSplit = 11;
    private int ivaratorMaxOpenFiles = 100;
    private int ivaratorNumRetries = 2;
    private boolean ivaratorPersistVerify = true;
    private int ivaratorPersistVerifyCount = 100;
    private long maxIvaratorSources = 33;
    private long maxIvaratorSourceWait = 1000L * 60 * 30;
    private long maxIvaratorResults = -1;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, true);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    public IvaratorConfig() {}

    public IvaratorConfig(IvaratorConfig other) {
        copyFrom(other);
    }

    public void copyFrom(IvaratorConfig other) {
        this.setIvaratorCacheDirConfigs((other.getIvaratorCacheDirConfigs() == null) ? null : new ArrayList<>(other.getIvaratorCacheDirConfigs()));
        this.setIvaratorFstHdfsBaseURIs(other.getIvaratorFstHdfsBaseURIs());
        this.setIvaratorCacheBufferSize(other.getIvaratorCacheBufferSize());
        this.setIvaratorCacheScanPersistThreshold(other.getIvaratorCacheScanPersistThreshold());
        this.setIvaratorCacheScanTimeout(other.getIvaratorCacheScanTimeout());
        this.setMaxFieldIndexRangeSplit(other.getMaxFieldIndexRangeSplit());
        this.setIvaratorMaxOpenFiles(other.getIvaratorMaxOpenFiles());
        this.setIvaratorNumRetries(other.getIvaratorNumRetries());
        this.setIvaratorPersistVerify(other.isIvaratorPersistVerify());
        this.setIvaratorPersistVerifyCount(other.getIvaratorPersistVerifyCount());
        this.setMaxIvaratorSources(other.getMaxIvaratorSources());

    }

    public static String toJson(IvaratorConfig IvaratorConfig) throws JsonProcessingException {
        return objectMapper.writeValueAsString(IvaratorConfig);
    }

    public static IvaratorConfig fromJson(String jsonArray) throws JsonProcessingException {
        return objectMapper.readValue(jsonArray, IvaratorConfig.class);
    }

    public List<IvaratorCacheDirConfig> getIvaratorCacheDirConfigs() {
        return ivaratorCacheDirConfigs;
    }

    public void setIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
        this.ivaratorCacheDirConfigs = ivaratorCacheDirConfigs;
    }

    public String getIvaratorFstHdfsBaseURIs() {
        return ivaratorFstHdfsBaseURIs;
    }

    public void setIvaratorFstHdfsBaseURIs(String ivaratorFstHdfsBaseURIs) {
        this.ivaratorFstHdfsBaseURIs = ivaratorFstHdfsBaseURIs;
    }

    public int getIvaratorCacheBufferSize() {
        return ivaratorCacheBufferSize;
    }

    public void setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.ivaratorCacheBufferSize = ivaratorCacheBufferSize;
    }

    public long getIvaratorCacheScanPersistThreshold() {
        return ivaratorCacheScanPersistThreshold;
    }

    public void setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.ivaratorCacheScanPersistThreshold = ivaratorCacheScanPersistThreshold;
    }

    public long getIvaratorCacheScanTimeout() {
        return ivaratorCacheScanTimeout;
    }

    public void setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.ivaratorCacheScanTimeout = ivaratorCacheScanTimeout;
    }

    public int getMaxFieldIndexRangeSplit() {
        return maxFieldIndexRangeSplit;
    }

    public void setMaxFieldIndexRangeSplit(int maxFieldIndexRangeSplit) {
        this.maxFieldIndexRangeSplit = maxFieldIndexRangeSplit;
    }

    public int getIvaratorMaxOpenFiles() {
        return ivaratorMaxOpenFiles;
    }

    public void setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.ivaratorMaxOpenFiles = ivaratorMaxOpenFiles;
    }

    public int getIvaratorNumRetries() {
        return ivaratorNumRetries;
    }

    public void setIvaratorNumRetries(int ivaratorNumRetries) {
        this.ivaratorNumRetries = ivaratorNumRetries;
    }

    public boolean isIvaratorPersistVerify() {
        return ivaratorPersistVerify;
    }

    public void setIvaratorPersistVerify(boolean ivaratorPersistVerify) {
        this.ivaratorPersistVerify = ivaratorPersistVerify;
    }

    public int getIvaratorPersistVerifyCount() {
        return ivaratorPersistVerifyCount;
    }

    public void setIvaratorPersistVerifyCount(int ivaratorPersistVerifyCount) {
        this.ivaratorPersistVerifyCount = ivaratorPersistVerifyCount;
    }

    public long getMaxIvaratorSources() {
        return maxIvaratorSources;
    }

    public void setMaxIvaratorSources(long maxIvaratorSources) {
        this.maxIvaratorSources = maxIvaratorSources;
    }

    public long getMaxIvaratorSourceWait() {
        return maxIvaratorSourceWait;
    }

    public void setMaxIvaratorSourceWait(long maxIvaratorSourceWait) {
        this.maxIvaratorSourceWait = maxIvaratorSourceWait;
    }

    public long getMaxIvaratorResults() {
        return maxIvaratorResults;
    }

    public void setMaxIvaratorResults(long maxIvaratorResults) {
        this.maxIvaratorResults = maxIvaratorResults;
    }
}
