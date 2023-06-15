package datawave.metrics.mapreduce.error;

import java.io.IOException;

import datawave.util.StringUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Mapper to produce counts from the processingErrors table.
 *
 *
 */
public class ProcessingErrorsMapper extends Mapper<Key,Value,Text,Text> {

    private static final Logger log = Logger.getLogger(ProcessingErrorsMapper.class);

    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        // format of the row == JobName\0DataType\0UID
        final String[] rowSplit = StringUtils.split(key.getRow().toString(), "\0");
        log.info("Have key " + key);
        final String cfString = key.getColumnFamily().toString();

        final String dataType = rowSplit[1];

        final String jobName = rowSplit[0];

        if ("e".equals(cfString)) {

            context.write(new Text(dataType + "\0" + jobName + "\0cnt"), new Text("1"));
        } else if ("info".equals(cfString)) {
            final String[] cqSplit = StringUtils.split(key.getColumnQualifier().toString(), "\0");

            context.write(new Text(dataType + "\0" + jobName + "\0" + cqSplit[0] + "\0infocnt"), new Text(cqSplit[1]));
        }

    }

}
