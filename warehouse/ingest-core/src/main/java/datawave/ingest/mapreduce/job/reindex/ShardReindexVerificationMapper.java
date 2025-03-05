package datawave.ingest.mapreduce.job.reindex;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.util.RFileUtil;
import datawave.ingest.protobuf.TermWeight;

public class ShardReindexVerificationMapper extends Mapper<Range,String,Key,Value> {
    private static final Logger log = Logger.getLogger(ShardReindexVerificationMapper.class);

    private AccumuloClient accumuloClient = null;
    private AccumuloHelper accumuloHelper = null;

    private Scanner scanner1;
    private Scanner scanner2;

    private Configuration config;

    void setAccumuloClient(AccumuloClient accumuloClient) {
        this.accumuloClient = accumuloClient;
    }

    @Override
    protected void setup(Context context) {
        config = context.getConfiguration();

        String source1TypeConfig = config.get("source1");
        String source2TypeConfig = config.get("source2");

        if (source1TypeConfig == null || source2TypeConfig == null) {
            throw new IllegalArgumentException("source1 and source2 must be set to a sourceType");
        }

        ShardReindexVerificationJob.SourceType source1 = ShardReindexVerificationJob.SourceType.valueOf(source1TypeConfig);
        ShardReindexVerificationJob.SourceType source2 = ShardReindexVerificationJob.SourceType.valueOf(source2TypeConfig);

        scanner1 = setupSource(source1, 1, config);
        scanner2 = setupSource(source2, 2, config);
    }

    private Scanner setupSource(ShardReindexVerificationJob.SourceType source, int sourceNum, Configuration config) {
        if (source == ShardReindexVerificationJob.SourceType.ACCUMULO) {
            String table = config.get("source" + sourceNum + ".table");

            if (table == null) {
                throw new IllegalArgumentException("source" + sourceNum + ".table table not set for ACCUMULO source");
            }

            return getScanner(source, table);
        } else {
            String paths = config.get("source" + sourceNum + ".files");

            if (paths == null) {
                throw new IllegalArgumentException("source" + sourceNum + ".files files not set for FILE source");
            }
            return getScanner(source, paths);
        }
    }

    private Scanner getScanner(ShardReindexVerificationJob.SourceType type, String sourceArg) {
        if (type == ShardReindexVerificationJob.SourceType.ACCUMULO) {
            if (accumuloHelper == null) {
                accumuloHelper = new AccumuloHelper();
                accumuloHelper.setup(config);
            }
            if (accumuloClient == null) {
                accumuloClient = accumuloHelper.newClient();
            }

            try {
                Authorizations auths = accumuloClient.securityOperations().getUserAuthorizations(accumuloHelper.getUsername());
                return accumuloClient.createScanner(sourceArg, auths);
            } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
                throw new RuntimeException("could not setup scanner on " + sourceArg, e);
            }
        } else {
            String[] paths = sourceArg.split(",");
            return RFileUtil.getRFileScanner(config, paths);
        }
    }

    @Override
    protected void map(Range range, String value, Context context) throws IOException, InterruptedException {
        scanner1.setRange(range);
        scanner2.setRange(range);

        Iterator<Map.Entry<Key,Value>> iterator1 = scanner1.iterator();
        Iterator<Map.Entry<Key,Value>> iterator2 = scanner2.iterator();

        Map.Entry<Key,Value> last1 = null;
        Map.Entry<Key,Value> last2 = null;
        boolean exhausted = false;
        while (!exhausted) {
            Map.Entry<Key,Value> entry1;
            Map.Entry<Key,Value> entry2;

            if ((last1 == null && last2 == null) || last1.equals(last2)) {
                entry1 = advance(iterator1);
                entry2 = advance(iterator2);
                context.getCounter("progress", "source1").increment(1);
                context.getCounter("progress", "source2").increment(1);
            } else {
                int keyDiff = last1.getKey().compareTo(last2.getKey());
                if (keyDiff < 0) {
                    entry1 = advance(iterator1);
                    entry2 = last2;
                    context.getCounter("progress", "source1").increment(1);
                } else if (keyDiff > 0) {
                    entry1 = last1;
                    entry2 = advance(iterator2);
                    context.getCounter("progress", "source2").increment(1);
                } else {
                    entry1 = advance(iterator1);
                    entry2 = advance(iterator2);
                    context.getCounter("progress", "source1").increment(1);
                    context.getCounter("progress", "source2").increment(1);
                }
            }

            if (entry1 == null || entry2 == null) {
                exhausted = true;
                if (entry1 == null && entry2 != null) {
                    writeDiff(context, 2, entry2.getKey(), entry2.getValue(), null, null);
                    context.getCounter("diff", "source2").increment(1);
                } else if (entry1 != null) {
                    writeDiff(context, 1, entry1.getKey(), entry1.getValue(), null, null);
                    context.getCounter("diff", "source1").increment(1);
                }

                continue;
            }

            if (!entry1.equals(entry2)) {
                int keyDiff = entry1.getKey().compareTo(entry2.getKey());
                if (keyDiff < 0) {
                    writeDiff(context, 1, entry1.getKey(), entry1.getValue(), entry2.getKey(), entry2.getValue());
                } else if (keyDiff > 0) {
                    writeDiff(context, 2, entry2.getKey(), entry2.getValue(), entry1.getKey(), entry1.getValue());
                } else {
                    writeDiff(context, 1, entry1.getKey(), entry1.getValue(), entry2.getKey(), entry2.getValue());
                }
            } else {
                context.progress();
            }

            last1 = entry1;
            last2 = entry2;
        }

        // after one iterator is exhausted gets here, write remaining iterator as a diff
        dumpIterator(1, iterator1, context);
        dumpIterator(2, iterator2, context);
    }

    private Map.Entry<Key,Value> advance(Iterator<Map.Entry<Key,Value>> itr) {
        if (itr.hasNext()) {
            return itr.next();
        }

        return null;
    }

    private void dumpIterator(int sourceNum, Iterator<Map.Entry<Key,Value>> itr, Context context) throws IOException, InterruptedException {
        while (itr.hasNext()) {
            Map.Entry<Key,Value> entry = itr.next();
            writeDiff(context, sourceNum, entry.getKey(), entry.getValue(), null, null);
        }
    }

    private void writeDiff(Context context, int sourceNum, Key key, Value value, Key otherKey, Value otherValue) throws IOException, InterruptedException {
        ByteSequence cf = key.getColumnFamilyData();
        if (ShardReindexMapper.isKeyD(cf)) {
            context.getCounter(key.getRow().toString() + "-" + sourceNum, "d").increment(1);
            log.info("D " + sourceNum + " " + key);
            log.info("OTHER " + otherKey);
        } else if (ShardReindexMapper.isKeyTF(cf)) {
            String field = ShardReindexMapper.getFieldFromTF(key);
            context.getCounter(key.getRow().toString() + "-" + sourceNum, "tf").increment(1);
            context.getCounter("tf", field).increment(1);
            TermWeight.Info info1 = TermWeight.Info.parseFrom(value.get());
            TermWeight.Info info2 = TermWeight.Info.parseFrom(value.get());
            log.info("TF " + key + " " + StringUtils.join(info1.getTermOffsetList(), ',') + " | " + StringUtils.join(info2.getTermOffsetList(), ','));
            context.getCounter("diff", "value").increment(1);
        } else if (ShardReindexMapper.isKeyFI(cf)) {
            String field = ShardReindexMapper.getFieldFromFI(key);
            context.getCounter(key.getRow().toString() + "-" + sourceNum, "fi").increment(1);
            context.getCounter("fi-" + sourceNum, field).increment(1);
            log.info("FI " + sourceNum + " " + key);
            log.info("OTHER " + otherKey);
        } else if (ShardReindexMapper.isKeyEvent(cf)) {
            context.getCounter(key.getRow().toString() + "-" + sourceNum, "event").increment(1);
            context.getCounter("event-" + sourceNum, cf.toString()).increment(1);
            log.info("EVENT " + sourceNum + " " + key);
            log.info("OTHER " + otherKey);
        } else {
            // index keys? TODO
        }

        context.write(key, value);
        context.getCounter("diff", "source" + sourceNum).increment(1);
    }

    @Override
    protected void cleanup(Context context) {
        if (accumuloClient != null) {
            accumuloClient.close();
        }
    }
}
