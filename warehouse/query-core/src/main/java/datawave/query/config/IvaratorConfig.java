package datawave.query.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

    public IvaratorConfig setIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
        this.ivaratorCacheDirConfigs = ivaratorCacheDirConfigs;
        return this;
    }

    public String getIvaratorFstHdfsBaseURIs() {
        return ivaratorFstHdfsBaseURIs;
    }

    public IvaratorConfig setIvaratorFstHdfsBaseURIs(String ivaratorFstHdfsBaseURIs) {
        this.ivaratorFstHdfsBaseURIs = ivaratorFstHdfsBaseURIs;
        return this;
    }

    public int getIvaratorCacheBufferSize() {
        return ivaratorCacheBufferSize;
    }

    public IvaratorConfig setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.ivaratorCacheBufferSize = ivaratorCacheBufferSize;
        return this;
    }

    public long getIvaratorCacheScanPersistThreshold() {
        return ivaratorCacheScanPersistThreshold;
    }

    public IvaratorConfig setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.ivaratorCacheScanPersistThreshold = ivaratorCacheScanPersistThreshold;
        return this;
    }

    public long getIvaratorCacheScanTimeout() {
        return ivaratorCacheScanTimeout;
    }

    public IvaratorConfig setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.ivaratorCacheScanTimeout = ivaratorCacheScanTimeout;
        return this;
    }

    public int getMaxFieldIndexRangeSplit() {
        return maxFieldIndexRangeSplit;
    }

    public IvaratorConfig setMaxFieldIndexRangeSplit(int maxFieldIndexRangeSplit) {
        this.maxFieldIndexRangeSplit = maxFieldIndexRangeSplit;
        return this;
    }

    public int getIvaratorMaxOpenFiles() {
        return ivaratorMaxOpenFiles;
    }

    public IvaratorConfig setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.ivaratorMaxOpenFiles = ivaratorMaxOpenFiles;
        return this;
    }

    public int getIvaratorNumRetries() {
        return ivaratorNumRetries;
    }

    public IvaratorConfig setIvaratorNumRetries(int ivaratorNumRetries) {
        this.ivaratorNumRetries = ivaratorNumRetries;
        return this;
    }

    public boolean isIvaratorPersistVerify() {
        return ivaratorPersistVerify;
    }

    public IvaratorConfig setIvaratorPersistVerify(boolean ivaratorPersistVerify) {
        this.ivaratorPersistVerify = ivaratorPersistVerify;
        return this;
    }

    public int getIvaratorPersistVerifyCount() {
        return ivaratorPersistVerifyCount;
    }

    public IvaratorConfig setIvaratorPersistVerifyCount(int ivaratorPersistVerifyCount) {
        this.ivaratorPersistVerifyCount = ivaratorPersistVerifyCount;
        return this;
    }

    public long getMaxIvaratorSources() {
        return maxIvaratorSources;
    }

    public IvaratorConfig setMaxIvaratorSources(long maxIvaratorSources) {
        this.maxIvaratorSources = maxIvaratorSources;
        return this;
    }

    public long getMaxIvaratorSourceWait() {
        return maxIvaratorSourceWait;
    }

    public IvaratorConfig setMaxIvaratorSourceWait(long maxIvaratorSourceWait) {
        this.maxIvaratorSourceWait = maxIvaratorSourceWait;
        return this;
    }

    public long getMaxIvaratorResults() {
        return maxIvaratorResults;
    }

    public IvaratorConfig setMaxIvaratorResults(long maxIvaratorResults) {
        this.maxIvaratorResults = maxIvaratorResults;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IvaratorConfig other = (IvaratorConfig) obj;
        return getIvaratorCacheBufferSize() == other.getIvaratorCacheBufferSize() &&
                getIvaratorCacheScanPersistThreshold() == other.getIvaratorCacheScanPersistThreshold() &&
                getIvaratorCacheScanTimeout() == other.getIvaratorCacheScanTimeout() &&
                getMaxFieldIndexRangeSplit() == other.getMaxFieldIndexRangeSplit() &&
                getIvaratorMaxOpenFiles() == other.getIvaratorMaxOpenFiles() &&
                getIvaratorNumRetries() == other.getIvaratorNumRetries() &&
                isIvaratorPersistVerify() == other.isIvaratorPersistVerify() &&
                getIvaratorPersistVerifyCount() == other.getIvaratorPersistVerifyCount() &&
                getMaxIvaratorSources() == other.getMaxIvaratorSources() &&
                getMaxIvaratorSourceWait() == other.getMaxIvaratorSourceWait() &&
                getMaxIvaratorResults() == other.getMaxIvaratorResults() &&
                (getIvaratorCacheDirConfigs() == null ? other.getIvaratorCacheDirConfigs() == null : getIvaratorCacheDirConfigs().equals(other.getIvaratorCacheDirConfigs())) &&
                (getIvaratorFstHdfsBaseURIs() == null ? other.getIvaratorFstHdfsBaseURIs() == null : getIvaratorFstHdfsBaseURIs().equals(other.getIvaratorFstHdfsBaseURIs()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getIvaratorCacheDirConfigs(),
                getIvaratorFstHdfsBaseURIs(),
                getIvaratorCacheBufferSize(),
                getIvaratorCacheScanPersistThreshold(),
                getIvaratorCacheScanTimeout(),
                getMaxFieldIndexRangeSplit(),
                getIvaratorMaxOpenFiles(),
                getIvaratorNumRetries(),
                isIvaratorPersistVerify(),
                getIvaratorPersistVerifyCount(),
                getMaxIvaratorSources(),
                getMaxIvaratorSourceWait(),
                getMaxIvaratorResults()
        );
    }


}
