package datawave.query.iterator.ivarator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class IvaratorCacheDirConfig implements Serializable {
    private static final Logger log = Logger.getLogger(IvaratorCacheDir.class);

    public static final int DEFAULT_PRIORITY = Integer.MAX_VALUE;
    public static final long DEFAULT_MIN_AVAILABLE_STORAGE_MiB = 0L;
    public static final double DEFAULT_MIN_AVAILABLE_STORAGE_PERCENT = 0f;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // the base path for caching ivarator output for this filesystem
    protected String basePathURI;

    // a number >= 0 used to determine the order in which ivarator cache dirs are used (ascending order)
    protected int priority;

    // the minimum amount of available storage required to use this filesystem
    protected long minAvailableStorageMiB;

    // the minimum percent of available storage required to use this filesystem
    protected double minAvailableStoragePercent;

    static {
        objectMapper.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, true);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    private IvaratorCacheDirConfig() {
        this(null);
    }

    public IvaratorCacheDirConfig(String basePathURI) {
        this(basePathURI, DEFAULT_PRIORITY);
    }

    public IvaratorCacheDirConfig(String basePathURI, int priority) {
        this(basePathURI, priority, DEFAULT_MIN_AVAILABLE_STORAGE_MiB, DEFAULT_MIN_AVAILABLE_STORAGE_PERCENT);
    }

    public IvaratorCacheDirConfig(String basePathURI, int priority, long minAvailableStorageMiB) {
        this(basePathURI, priority, minAvailableStorageMiB, DEFAULT_MIN_AVAILABLE_STORAGE_PERCENT);
    }

    public IvaratorCacheDirConfig(String basePathURI, int priority, double minAvailableStoragePercent) {
        this(basePathURI, priority, DEFAULT_MIN_AVAILABLE_STORAGE_MiB, minAvailableStoragePercent);
    }

    private IvaratorCacheDirConfig(String basePathURI, int priority, long minAvailableStorageMiB, double minAvailableStoragePercent) {
        this.basePathURI = basePathURI;
        this.priority = priority;
        this.minAvailableStorageMiB = minAvailableStorageMiB;
        this.minAvailableStoragePercent = minAvailableStoragePercent;
    }

    @JsonIgnore
    public boolean isValid() {
        boolean result = true;

        if (basePathURI == null || (!basePathURI.startsWith("file:/") && !basePathURI.startsWith("hdfs:/"))) {
            log.warn("Invalid basePathURI for IvaratorCacheDirConfig.  'basePathURI' scheme must be either 'file:' or 'hdfs:'");
            result = false;
        }

        if (minAvailableStorageMiB < 0l) {
            log.warn("Invalid minAvailableStorageMB for IvaratorCacheDirConfig.  'minAvailableStorageMB' must be greater than or equal to 0");
            result = false;
        }

        if (minAvailableStoragePercent < 0.0 || minAvailableStoragePercent > 1.0) {
            log.warn("Invalid minAvailableStoragePercent for IvaratorCacheDirConfig.  'minAvailableStoragePercent' must be between 0.0 and 1.0 inclusive");
            result = false;
        }

        return result;
    }

    public String getBasePathURI() {
        return basePathURI;
    }

    public void setBasePathURI(String basePathURI) {
        this.basePathURI = basePathURI;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getMinAvailableStorageMiB() {
        return minAvailableStorageMiB;
    }

    public void setMinAvailableStorageMiB(long minAvailableStorageMiB) {
        this.minAvailableStorageMiB = minAvailableStorageMiB;
    }

    public double getMinAvailableStoragePercent() {
        return minAvailableStoragePercent;
    }

    public void setMinAvailableStoragePercent(double minAvailableStoragePercent) {
        this.minAvailableStoragePercent = minAvailableStoragePercent;
    }

    public static String toJson(IvaratorCacheDirConfig ivaratorCacheDirConfig) throws JsonProcessingException {
        return toJson(Collections.singletonList(ivaratorCacheDirConfig));
    }

    public static String toJson(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) throws JsonProcessingException {
        return objectMapper.writeValueAsString(ivaratorCacheDirConfigs);
    }

    public static List<IvaratorCacheDirConfig> fromJson(String jsonArray) throws JsonProcessingException {
        return new ArrayList<>(Arrays.asList(objectMapper.readValue(jsonArray, IvaratorCacheDirConfig[].class)));
    }

    @Override
    public String toString() {
        return "IvaratorCacheDirConfig: [basePathURI: " + basePathURI + ", priority: " + priority + ", minAvailableStorageMiB: " + minAvailableStorageMiB
                        + ", minAvailableStoragePercent: " + minAvailableStoragePercent + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof IvaratorCacheDirConfig))
            return false;
        IvaratorCacheDirConfig that = (IvaratorCacheDirConfig) o;
        return priority == that.priority && minAvailableStorageMiB == that.minAvailableStorageMiB
                        && Double.compare(that.minAvailableStoragePercent, minAvailableStoragePercent) == 0 && Objects.equals(basePathURI, that.basePathURI);
    }

    @Override
    public int hashCode() {
        return Objects.hash(basePathURI, priority, minAvailableStorageMiB, minAvailableStoragePercent);
    }
}
