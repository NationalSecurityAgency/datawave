package datawave.data.hash;

import static datawave.data.hash.UIDConstants.CONFIG_MACHINE_ID_KEY;
import static datawave.data.hash.UIDConstants.CONFIG_UID_TYPE_KEY;
import static datawave.data.hash.UIDConstants.HOST_INDEX_OPT;
import static datawave.data.hash.UIDConstants.PROCESS_INDEX_OPT;
import static datawave.data.hash.UIDConstants.THREAD_INDEX_OPT;
import static datawave.data.hash.UIDConstants.UID_TYPE_OPT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class UIDTest {
    @Test
    public void testBuilderCreation() {
        // Test default building, which should result in a Hash-based builder
        UIDBuilder<UID> result1 = UID.builder();
        assertSame(result1.getClass(), HashUIDBuilder.class);
        assertSame(result1, UID.builder());
        
        // Test default building due to a null Configuration
        UIDBuilder<UID> result2 = UID.builder((Configuration) null);
        assertSame(result2.getClass(), HashUIDBuilder.class);
        assertSame(result2, UID.builder());
        
        // Test default building due to a null Date
        UIDBuilder<UID> result3 = UID.builder((Date) null);
        assertSame(result3.getClass(), HashUIDBuilder.class);
        assertSame(result3, UID.builder());
        
        // Test creation of a non-default, hash-based builder due to a non-null Date
        UIDBuilder<UID> result4 = UID.builder(new Date());
        assertSame(result4.getClass(), HashUIDBuilder.class);
        assertNotSame(result4, UID.builder());
        
        // Test default building due to a Configuration with a missing uidType property
        UIDBuilder<UID> result5 = UID.builder(new Configuration());
        assertSame(result5.getClass(), HashUIDBuilder.class);
        assertSame(result5, UID.builder());
        
        // Test default building due to a Configuration with a HashUID uidType
        Configuration configuration = new Configuration();
        configuration.set(CONFIG_UID_TYPE_KEY, HashUID.class.getSimpleName());
        UIDBuilder<UID> result6 = UID.builder(configuration);
        assertSame(result6.getClass(), HashUIDBuilder.class);
        assertSame(result6, UID.builder());
        
        // Test throwing of an exception due to a Configuration with a SnowflakeUID uidType
        // but a missing machine ID
        configuration = new Configuration();
        configuration.set(CONFIG_UID_TYPE_KEY, SnowflakeUID.class.getSimpleName());
        Exception result7 = null;
        try {
            UID.builder(configuration);
        } catch (IllegalArgumentException e) {
            result7 = e;
        }
        assertNotNull(result7);
        
        // Test creation of a non-default, Snowflake-based builder based on a 20-bit machine ID
        // defined as node 30, process 20, and thread 10: (30 << 12) + (20 << 6) + 10). Such a
        // machine ID translates into decimal value 124170 and hexadecimal value 1e50a.
        configuration = new Configuration();
        configuration.set(CONFIG_UID_TYPE_KEY, SnowflakeUID.class.getSimpleName());
        configuration.setInt(CONFIG_MACHINE_ID_KEY, ((30 << 12) + (20 << 6) + 10));
        UIDBuilder<UID> result8 = UID.builder(configuration, new Date());
        assertSame(result8.getClass(), SnowflakeUIDBuilder.class);
        assertNotSame(result8, UID.builder());
        
        // Test throwing of an exception due to a Configuration with a SnowflakeUID uidType
        // but an invalid machine ID (the first 8-bit node portion of the ID exceeds 255)
        configuration = new Configuration();
        configuration.set(CONFIG_UID_TYPE_KEY, SnowflakeUID.class.getSimpleName());
        configuration.setInt(CONFIG_MACHINE_ID_KEY, ((256 << 12) + (20 << 6) + 10));
        Exception result9 = null;
        try {
            UID.builder(configuration);
        } catch (IllegalArgumentException e) {
            result9 = e;
        }
        assertNotNull(result9);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testConfigurationViaBuilder() {
        // Test all nulls
        UIDBuilder<UID> builder = UID.builder();
        builder.configure(null, (Option[]) null);
        
        // Test null options
        Configuration configuration = new Configuration();
        builder.configure(configuration, (Option[]) null);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test individual null options
        configuration = new Configuration();
        builder.configure(configuration, null, null);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test distractor option
        configuration = new Configuration();
        Option distractorOption = new Option("distractor", "distractor", true, "distractor");
        distractorOption.setRequired(false);
        distractorOption.setArgs(1);
        distractorOption.setType(Boolean.class);
        builder.configure(configuration, null, distractorOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test HashUID uidType option
        configuration = new Configuration();
        Option uidTypeOption = new Option(UID_TYPE_OPT, UID_TYPE_OPT, true, "UID type configuration (default HashUID)");
        uidTypeOption.setRequired(false);
        uidTypeOption.setArgs(1);
        uidTypeOption.setType(String.class);
        uidTypeOption.getValuesList().add(HashUID.class.getSimpleName());
        builder.configure(configuration, uidTypeOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test unrecognized uidType option
        configuration = new Configuration();
        uidTypeOption.getValuesList().clear();
        uidTypeOption.getValuesList().add("bogus");
        builder.configure(configuration, uidTypeOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test host index option in isolation
        configuration = new Configuration();
        Option hostIndexOption = new Option(HOST_INDEX_OPT, HOST_INDEX_OPT, true, "Host index");
        hostIndexOption.setRequired(false);
        hostIndexOption.setArgs(1);
        hostIndexOption.setType(String.class);
        hostIndexOption.getValuesList().add(Integer.toString(30));
        builder.configure(configuration, hostIndexOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test SnowflakeUID uidType option missing the other required options: host, process, and thread indices
        configuration = new Configuration();
        uidTypeOption.getValuesList().clear();
        uidTypeOption.getValuesList().add(SnowflakeUID.class.getSimpleName());
        builder.configure(configuration, uidTypeOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test incomplete SnowflakeUID configuration due to excessive thread index value
        configuration = new Configuration();
        Option processIndexOption = new Option(PROCESS_INDEX_OPT, PROCESS_INDEX_OPT, true, "Process index");
        processIndexOption.setRequired(false);
        processIndexOption.setArgs(1);
        processIndexOption.setType(String.class);
        processIndexOption.getValuesList().add(Integer.toString(20));
        Option threadIndexOption = new Option(THREAD_INDEX_OPT, THREAD_INDEX_OPT, true, "Thread index");
        threadIndexOption.setRequired(false);
        threadIndexOption.setArgs(1);
        threadIndexOption.setType(String.class);
        threadIndexOption.getValuesList().add(Integer.toString(64));
        builder.configure(configuration, hostIndexOption, distractorOption, threadIndexOption, uidTypeOption, processIndexOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test complete SnowflakeUID configuration, including the ignored distractor option
        configuration = new Configuration();
        threadIndexOption.getValuesList().clear();
        threadIndexOption.getValuesList().add(Integer.toString(10));
        builder.configure(configuration, hostIndexOption, distractorOption, threadIndexOption, uidTypeOption, processIndexOption);
        assertEquals(SnowflakeUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test invalid SnowflakeUID configuration due to non-integer host index
        configuration = new Configuration();
        hostIndexOption.getValuesList().clear();
        hostIndexOption.getValuesList().add("bogus");
        builder.configure(configuration, hostIndexOption, distractorOption, threadIndexOption, uidTypeOption, processIndexOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test invalid SnowflakeUID configuration due to non-integer process index
        configuration = new Configuration();
        hostIndexOption.getValuesList().clear();
        hostIndexOption.getValuesList().add(Integer.toString(30));
        processIndexOption.getValuesList().clear();
        processIndexOption.getValuesList().add("bogus");
        builder.configure(configuration, hostIndexOption, distractorOption, threadIndexOption, uidTypeOption, processIndexOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
        
        // Test invalid SnowflakeUID configuration due to non-integer thread index
        configuration = new Configuration();
        processIndexOption.getValuesList().clear();
        processIndexOption.getValuesList().add(Integer.toString(20));
        threadIndexOption.getValuesList().clear();
        threadIndexOption.getValuesList().add("bogus");
        builder.configure(configuration, hostIndexOption, distractorOption, threadIndexOption, uidTypeOption, processIndexOption);
        assertEquals(HashUID.class.getSimpleName(), configuration.get(CONFIG_UID_TYPE_KEY));
    }
    
    @Test
    public void testComparisons() {
        long timestamp = 1449585658444L;
        SnowflakeUIDBuilder builder = SnowflakeUID.builder(timestamp, 1, 1, 1);
        
        HashUID uid1 = new HashUID("test".getBytes(), new Date(timestamp));
        SnowflakeUID uid2 = builder.newId();
        SnowflakeUID uid3 = builder.newId();
        SnowflakeUID uid4 = builder.newId(uid3.getSequenceId(), "1");
        SnowflakeUID uid5 = builder.newId(uid3.getSequenceId(), "10", "1");
        SnowflakeUID uid6 = builder.newId(uid3.getSequenceId(), "2");
        HashUID uid7 = new HashUID();
        SnowflakeUID uid8 = new SnowflakeUID(null, 16, "1");
        
        List<UID> list = new ArrayList<>();
        list.add(null);
        list.add(uid8);
        list.add(uid7);
        list.add(uid6);
        list.add(uid5);
        list.add(uid4);
        list.add(uid3);
        list.add(uid2);
        list.add(uid1);
        Collections.sort(list);
        
        assertEquals(0, list.indexOf(uid1));
        assertEquals(1, list.indexOf(uid2));
        assertEquals(2, list.indexOf(uid3));
        assertEquals(3, list.indexOf(uid4));
        assertEquals(4, list.indexOf(uid5));
        assertEquals(5, list.indexOf(uid6));
        assertEquals(6, list.indexOf(uid7));
        assertEquals(7, list.indexOf(uid8));
    }
    
    @Test
    public void testEquality() {
        long timestamp = 1449585658444L;
        SnowflakeUIDBuilder builder = SnowflakeUID.builder(timestamp, 1, 1, 1);
        
        HashUID uid1 = new HashUID("test".getBytes(), new Date(timestamp));
        SnowflakeUID uid2 = builder.newId();
        SnowflakeUID uid3 = builder.newId();
        SnowflakeUID uid4 = builder.newId(uid3.getSequenceId());
        SnowflakeUID uid5 = builder.newId(uid3.getSequenceId(), "1");
        SnowflakeUID uid6 = builder.newId(uid3.getSequenceId(), "1.1");
        HashUID uid7 = new HashUID();
        SnowflakeUID uid8 = new SnowflakeUID(null, 16, "1");
        
        assertNotEquals(null, uid1);
        assertEquals(uid1, uid1);
        assertNotEquals(uid1, uid2);
        
        assertEquals(uid2, uid2);
        assertNotEquals(uid2, uid3);
        
        assertEquals(uid3, uid4);
        
        assertNotEquals(uid4, uid5);
        
        assertNotEquals(uid5, uid6);
        
        assertNotEquals(uid6, uid7);
        
        assertNotEquals(uid7, uid8);
        
        assertEquals(uid8, uid8);
    }
    
    @Test
    public void testParsing() {
        long timestamp = 1449585658444L;
        
        // Test hashes
        HashUID uid1 = new HashUID("test".getBytes(), new Date(timestamp));
        assertEquals(HashUID.extractTimeOfDay(new Date(timestamp)), uid1.getTime());
        HashUID parsed1 = UID.parse(uid1.toString());
        assertEquals(uid1, parsed1);
        assertEquals(uid1.getTime(), parsed1.getTime());
        
        HashUID uid2 = new HashUID("test".getBytes(), new Date(timestamp), "1");
        HashUID parsed2 = UID.parse(uid2.toString());
        assertEquals(uid2, parsed2);
        assertEquals(uid2.getExtra(), parsed2.getExtra());
        
        HashUID uid3 = new HashUID();
        HashUID parsed3 = UID.parse(uid3.toString());
        assertEquals(uid3, parsed3);
        
        // Test snowflakes
        SnowflakeUIDBuilder builder = SnowflakeUID.builder(timestamp, 10, 20, 30);
        
        SnowflakeUID uid4 = builder.newId(0L); // Throwing in a zero value timestamp to check for Hash vs. Snowflake collision
        SnowflakeUID parsed4 = UID.parse(uid4.toString());
        assertEquals(uid4, parsed4);
        
        SnowflakeUID uid5 = builder.newId(1111, "1", "2", "3");
        SnowflakeUID parsed5 = UID.parse(uid5.toString());
        assertEquals(uid5, parsed5);
        
        SnowflakeUID uid6 = builder.newId(1111, "1", "2", "3", "4");
        SnowflakeUID parsed6 = UID.parse(uid6.toString(), 3);
        assertEquals(uid5, parsed6);
        
        SnowflakeUID uid7 = builder.newId(1111);
        SnowflakeUID parsed7 = UID.parse(uid6.toString(), 0);
        assertEquals(uid7, parsed7);
        
        SnowflakeUID uid8 = builder.newId("1", "2", "3", "4");
        SnowflakeUID parsed8 = UID.parse(uid8.toString(), 0);
        assertEquals(1112, uid8.getSequenceId());
        assertEquals(uid8.getBaseUid(), parsed8.toString());
        
        SnowflakeUID uid9 = new SnowflakeUID(null, SnowflakeUID.DEFAULT_RADIX);
        SnowflakeUID parsed9 = UID.parse(uid9.toString(), 0);
        assertEquals(-1, uid9.getSequenceId());
        assertEquals(uid9.getBaseUid(), parsed9.toString());
        
        // Test parse base
        SnowflakeUID uid10 = UID.parseBase(uid9.toString());
        assertEquals(uid9.getBaseUid(), uid10.toString());
        Exception exception = null;
        try {
            UID.parseBase(null);
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        assertNotNull(exception);
        exception = null;
        try {
            UID.parseBase("");
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        assertNotNull(exception);
    }
    
    @Test
    public void testCopyFromTemplates() {
        // Test cloning of simple HashUID
        UIDBuilder<UID> builder = UID.builder();
        UID template = builder.newId();
        UID result1 = builder.newId(template);
        assertNotSame(result1, template);
        assertEquals(result1, template);
        
        // Test cloning of date-specified HashUID
        template = builder.newId(new Date());
        UID result2 = builder.newId(template);
        assertNotSame(result2, template);
        assertEquals(result2, template);
        
        // Test cloning of byte and date-specified HashUID
        template = builder.newId("test".getBytes(), new Date());
        UID result3 = builder.newId(template);
        assertNotSame(result3, template);
        assertEquals(result3, template);
        
        // Test cloning of byte and date-specified HashUID with extras
        template = builder.newId("test".getBytes(), new Date(), "1", "2");
        UID result4 = builder.newId(template);
        assertNotSame(result4, template);
        assertEquals(result4, template);
        
        // Test creation of child from byte and date-specified HashUID
        template = builder.newId("test".getBytes(), new Date());
        UID result5 = builder.newId(template, "1");
        assertNotSame(result5, template);
        assertNotEquals(result5, template);
        assertEquals(result5.toString(), template + ".1");
        
        // Test creation of child from byte and date-specified HashUID that already has extras
        template = builder.newId("test".getBytes(), new Date(), "1", "2");
        UID result6 = builder.newId(template, "3");
        assertNotSame(result6, template);
        assertNotEquals(result6, template);
        assertEquals(result6.toString(), template + ".3");
        
        // Test creation of child from custom HashUID that already has extras
        template = CustomHashUID.parse(template.toString());
        UID result7 = builder.newId(template, "3");
        assertNotSame(result7, template);
        assertNotEquals(result7, template);
        assertEquals(result7.toString(), template + ".3");
        
        // Test creation of child from previous custom HashUID using a SnowflakeUID builder
        Configuration configuration = new Configuration();
        configuration.set(CONFIG_UID_TYPE_KEY, SnowflakeUID.class.getSimpleName());
        configuration.setInt(CONFIG_MACHINE_ID_KEY, ((9 << 12) + (8 << 6) + 7)); // Machine ID: Node 9, Process 8, Thread 7
        builder = UID.builder(configuration);
        UID result8 = builder.newId(template, "3");
        assertNotSame(result8, template);
        assertNotEquals(result8, template);
        assertEquals(result8.toString(), template + ".3");
        
        // Test cloning of simple SnowflakeUID
        template = builder.newId();
        UID result9 = builder.newId(template);
        assertNotSame(result9, template);
        assertEquals(result9, template);
        
        // Test cloning of date-specified SnowflakeUID
        template = builder.newId(new Date());
        UID result10 = builder.newId(template);
        assertNotSame(result10, template);
        assertEquals(result10, template);
        
        // Test cloning of byte and date-specified SnowflakeUID
        template = builder.newId("test".getBytes(), new Date());
        UID result11 = builder.newId(template);
        assertNotSame(result11, template);
        assertEquals(result11, template);
        
        // Test cloning of byte and date-specified SnowflakeUID with extras
        template = builder.newId("test".getBytes(), new Date(), "1", "2");
        UID result12 = builder.newId(template);
        assertNotSame(result12, template);
        assertEquals(result12, template);
        
        // Test creation of child from byte and date-specified SnowflakeUID
        template = builder.newId("test".getBytes(), new Date());
        UID result13 = builder.newId(template, "1");
        assertNotSame(result13, template);
        assertNotEquals(result13, template);
        assertEquals(result13.toString(), template + ".1");
        
        // Test creation of child from byte and date-specified SnowflakeUID that already has extras
        template = builder.newId("test".getBytes(), new Date(), "1", "2");
        UID result14 = builder.newId(template, "3");
        assertNotSame(result14, template);
        assertNotEquals(result14, template);
        assertEquals(result14.toString(), template + ".3");
        
        // Test creation of child from custom HashUID that already has extras
        template = CustomSnowflakeUID.parse(template.toString());
        UID result15 = builder.newId(template, "3");
        assertNotSame(result15, template);
        assertNotEquals(result15, template);
        assertEquals(result15.toString(), template + ".3");
        
        // Test cloning of null template
        UID result16 = builder.newId((UID) null);
        assertNull(result16);
        
        // Test cloning of null template explicitly using SnowflakeUIDBuilder
        UID result17 = SnowflakeUIDBuilder.newId((SnowflakeUID) null);
        assertNull(result17);
    }
    
    @Test
    public void testMiscellaneous() {
        // Test null prefix
        Exception result1 = null;
        try {
            new TestUID(null, false, (String) null);
        } catch (IllegalArgumentException e) {
            result1 = e;
        }
        assertNotNull(result1);
        
        // Test single null extra
        UID result2 = UID.builder().newId("test".getBytes(), (String) null);
        assertNull(result2.getExtra());
        
        // Test single null extra array
        UID result3 = UID.builder().newId("test".getBytes(), (String[]) null);
        assertNull(result3.getExtra());
    }
    
    /*
     * Test subclass akin to the StringUID
     */
    private static class CustomHashUID extends HashUID {
        public CustomHashUID(HashUID template, String... extras) {
            super(template, extras);
        }
        
        public static HashUID parse(String s) {
            HashUID hashUid = HashUID.parse(s);
            if (null != hashUid) {
                return new CustomHashUID(hashUid);
            }
            
            return null;
        }
    }
    
    /*
     * Another test subclass akin to the StringUID
     */
    private static class CustomSnowflakeUID extends SnowflakeUID {
        public CustomSnowflakeUID(SnowflakeUID template, String... extras) {
            super(template, extras);
        }
        
        public static SnowflakeUID parse(String s) {
            SnowflakeUID hashUid = SnowflakeUID.parse(s);
            if (null != hashUid) {
                return new CustomSnowflakeUID(hashUid);
            }
            
            return null;
        }
    }
    
    private class TestUID extends UID {
        public TestUID(String prefix, boolean isPrefixOptional, String... extras) {
            super(prefix, isPrefixOptional, extras);
        }
        
        @Override
        public int compareTo(UID o) {
            return 0;
        }
        
        @Override
        public int getTime() {
            return 0;
        }
        
        @Override
        public String getShardedPortion() {
            return null;
        }
        
        @Override
        public String getBaseUid() {
            return null;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            // No op
        }
    }
}
