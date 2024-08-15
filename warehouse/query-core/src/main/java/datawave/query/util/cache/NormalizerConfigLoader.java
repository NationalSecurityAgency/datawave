package datawave.query.util.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.Type;
import datawave.ingest.util.cache.Loader;

/**
 *
 */
public class NormalizerConfigLoader extends Loader<String,Multimap<String,Type<?>>> {

    protected Collection<String> dataTypeFilters;

    private static final Logger log = Logger.getLogger(NormalizerConfigLoader.class);

    public NormalizerConfigLoader(final Configuration conf, String configItem) throws IOException {
        String configValue = conf.get(configItem);
        if (null != configValue) {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(Base64.getDecoder().decode(configValue));
            GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
            DataInputStream dataIn = new DataInputStream(gzipStream);
            readFields(dataIn);
            dataIn.close();
        } else
            throw new IllegalArgumentException("Configuration item does not exist");

    }

    public NormalizerConfigLoader() {

        this.dataTypeFilters = new ArrayList<>();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
     */
    @Override
    public Multimap<String,Type<?>> load(String key) throws Exception {
        super.load(key);
        return entryCache.get(key);

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.util.cache.Loader#build(java.lang.Object)
     */
    @Override
    protected void build(String key) throws Exception {
        // special case here. we must build the children early
        buildChildren(key);
    }

    public void store(Configuration conf) {

    }

    @Override
    public String toString() {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipStream;
        try {
            gzipStream = new GZIPOutputStream(byteStream);

            DataOutputStream dataOut = new DataOutputStream(gzipStream);
            write(dataOut);

            dataOut.close();

            return Base64.getEncoder().encodeToString(byteStream.toByteArray());
        } catch (IOException e) {
            log.error(e);
        }

        return "";
    }

    protected void write(DataOutput out) throws IOException {

        synchronized (entryCache) {
            int totalSize = 0;
            for (Entry<String,Multimap<String,Type<?>>> norm : entryCache.entrySet()) {
                totalSize += norm.getValue().size();
            }
            out.writeInt(totalSize);

            for (Entry<String,Multimap<String,Type<?>>> norm : entryCache.entrySet()) {
                for (Entry<String,Type<?>> entry : norm.getValue().entries()) {
                    out.writeUTF(norm.getKey());
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue().getClass().getCanonicalName());
                }
            }

        }

    }

    protected void readFields(DataInput in) throws IOException {

        int normalizerMapSize = in.readInt();

        for (int i = 0; i < normalizerMapSize; i++) {
            String fieldName = in.readUTF();
            String dataType = in.readUTF();
            String className = in.readUTF();

            try {

                Multimap<String,Type<?>> normalizerMap = entryCache.get(fieldName);
                if (null == normalizerMap) {
                    normalizerMap = ArrayListMultimap.create();

                    entryCache.put(fieldName, normalizerMap);
                }

                Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) Class.forName(className);

                normalizerMap.put(dataType, clazz.newInstance());
            } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
                log.error(e);
            }
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.util.cache.Loader#merge(java.lang.Object, java.lang.Object)
     */
    @Override
    protected void merge(String key, Multimap<String,Type<?>> value) throws Exception {
        entryCache.get(key).putAll(value);
    }

}
