package datawave.query.util;

import datawave.util.TableName;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

public class DateIndexTestIngest {
    
    protected static final String datatype = "test";
    protected static final ColumnVisibility viz = new ColumnVisibility("HUSH");
    
    protected SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    protected String[] rowValues = {"20100704", "20100703"};
    
    public static void writeItAll(Connector con) throws Exception {
        
        BatchWriter bw = null;
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        Mutation mutation = null;
        
        try {
            // write the date index table :
            bw = con.createBatchWriter(TableName.DATE_INDEX, bwConfig);
            
            BitSet zeroOne = new BitSet();
            zeroOne.set(0);
            
            BitSet zeroOneTwo = new BitSet();
            zeroOneTwo.set(0);
            zeroOneTwo.set(2);
            
            BitSet zeroOneTwoThree = new BitSet();
            zeroOneTwoThree.set(1);
            
            mutation = new Mutation("20100704");
            // 2 days before
            mutation.put(new Text("ACTIVITY"), new Text("20100702" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(zeroOne.toByteArray()));
            
            // day before
            mutation.put(new Text("ACTIVITY"), new Text("20100703" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(zeroOne.toByteArray()));
            
            // same day
            mutation.put(new Text("ACTIVITY"), new Text("20100704" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(zeroOneTwo.toByteArray()));
            
            // day after
            mutation.put(new Text("ACTIVITY"), new Text("20100705" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(zeroOneTwoThree.toByteArray()));
            
            bw.addMutation(mutation);
            
            mutation = new Mutation("20100101");
            BitSet one = new BitSet();
            one.set(1);
            mutation.put(new Text("ACTIVITY"), new Text("20100101" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(one.toByteArray()));
            
            BitSet twoFourFive = new BitSet();
            twoFourFive.set(2);
            twoFourFive.set(4);
            twoFourFive.set(5);
            mutation.put(new Text("ACTIVITY"), new Text("20100102" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(twoFourFive.toByteArray()));
            
            bw.addMutation(mutation);
            
            mutation = new Mutation("20100102");
            BitSet two = new BitSet();
            two.set(2);
            mutation.put(new Text("ACTIVITY"), new Text("20100102" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(two.toByteArray()));
            
            bw.addMutation(mutation);
            
            mutation = new Mutation("20100103");
            BitSet four = new BitSet();
            four.set(4);
            mutation.put(new Text("ACTIVITY"), new Text("20100103" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(four.toByteArray()));
            
            bw.addMutation(mutation);
            
            mutation = new Mutation("20100104");
            BitSet five = new BitSet();
            five.set(5);
            mutation.put(new Text("ACTIVITY"), new Text("20100104" + "\u0000" + datatype + "\u0000" + "UPTIME"), viz, new Value(five.toByteArray()));
            
            mutation.put(new Text("ACTIVITY"), new Text("20100702" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz, new Value(zeroOne.toByteArray()));
            
            // day before
            mutation.put(new Text("ACTIVITY"), new Text("20100703" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz, new Value(zeroOne.toByteArray()));
            
            // same day
            mutation.put(new Text("ACTIVITY"), new Text("20100704" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz,
                            new Value(zeroOneTwo.toByteArray()));
            
            // day after
            mutation.put(new Text("ACTIVITY"), new Text("20100705" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz,
                            new Value(zeroOneTwoThree.toByteArray()));
            
            bw.addMutation(mutation);
            
            mutation = new Mutation("20100704_0");
            // 2 days before
            mutation.put(new Text("ACTIVITY"), new Text("20100702" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz, new Value(zeroOne.toByteArray()));
            
            // day before
            mutation.put(new Text("ACTIVITY"), new Text("20100703" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz, new Value(zeroOne.toByteArray()));
            
            // same day
            mutation.put(new Text("ACTIVITY"), new Text("20100704" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz,
                            new Value(zeroOneTwo.toByteArray()));
            
            // day after
            mutation.put(new Text("ACTIVITY"), new Text("20100705" + "\u0000" + datatype + "\u0000" + "ACTIVITY_DATE"), viz,
                            new Value(zeroOneTwoThree.toByteArray()));
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
    }
}
