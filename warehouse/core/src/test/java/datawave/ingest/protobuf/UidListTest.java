package datawave.data.hash;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import datawave.ingest.protobuf.Uid;
//import datawave.ingest.protobuf.Uid.List.Builder;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.io.OutStream;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static datawave.data.hash.UIDConstants.CONFIG_MACHINE_ID_KEY;
import static datawave.data.hash.UIDConstants.CONFIG_UID_TYPE_KEY;
import static datawave.data.hash.UIDConstants.HOST_INDEX_OPT;
import static datawave.data.hash.UIDConstants.PROCESS_INDEX_OPT;
import static datawave.data.hash.UIDConstants.THREAD_INDEX_OPT;
import static datawave.data.hash.UIDConstants.UID_TYPE_OPT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class UidListTest {
    private static int ARRAY_SIZE = 10;
    private static UUID[] uid = new UUID[ARRAY_SIZE];
    private Uid.List.Builder builder;
    
    private final int IGNORE = 8;
    private final int COUNT = (byte) 0x10;
    private final int ADD_UID = (byte) 26;
    private final int REMOVE_UID = (byte) 34;
    private final int DONE = (byte) 0;
    
    @BeforeClass
    public static void seUpClasss() {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            uid[i] = UUID.randomUUID();
        }
    }
    
    @Before
    public void setUp() {
        builder = Uid.List.newBuilder();
    }
    
    private Uid.List.Builder createNewUidList() {
        return Uid.List.newBuilder();
    }
    
    private Uid.List.Builder createNewUidList(String... uidsToAdd) {
        Uid.List.Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(uidsToAdd.length);
        Arrays.stream(uidsToAdd).forEach(b::addUID);
        return b;
    }
    
    private Uid.List.Builder createNewRemoveUidList(String... uidsToRemove) {
        Uid.List.Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(-uidsToRemove.length);
        Arrays.stream(uidsToRemove).forEach(b::addREMOVEDUID);
        return b;
    }
    
    private Value toValue(Uid.List.Builder uidListBuilder) {
        return new Value(uidListBuilder.build().toByteArray());
    }
    
    // @Test
    // public void testSingleUid() {
    // String uuid = UUID.randomUUID().toString();
    // Value val = toValue(createNewUidList(uuid));
    //
    //
    // Uid.List result = new Uid.List(val.
    // assertNotNull(result);
    // assertNotNull(result.get());
    // assertNotNull(val.get());
    // assertEquals(0, val.compareTo(result.get()));
    // }
    
    @Test
    public void test1() {
        builder.addUID(uid[0].toString());
        builder.addUID(uid[1].toString());
        builder.addUID(uid[2].toString());
    }
    
    @Test
    public void test2() {
        builder.addUID(uid[0].toString());
        builder.addUID(uid[1].toString());
        builder.addUID(uid[2].toString());
        builder.addREMOVEDUID(uid[1].toString());
    }
    
    @Test
    public void test3() {
        builder.addUID(uid[0].toString());
        builder.addUID(uid[1].toString());
        builder.addUID(uid[0].toString());
        builder.addUID(uid[2].toString());
        builder.addUID(uid[0].toString());
    }
    
    @Test
    public void test4() {
        builder.addUID(uid[0].toString());
        builder.addUID(uid[0].toString());
    }
    
    private int addUidsSet1(CodedOutputStream outStream) throws IOException {
        int totalWritten = 0;
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[0].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[0].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[1].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[1].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[2].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[2].toString());
        
        return totalWritten;
    }
    
    private int addUidsSet2(CodedOutputStream outStream) throws IOException {
        int totalWritten = 0;
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[2].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[2].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[1].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[1].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[0].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[0].toString());
        
        return totalWritten;
    }
    
    private int addUidsSet3(CodedOutputStream outStream) throws IOException {
        int totalWritten = 0;
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[2].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[2].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[1].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[1].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[1].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[1].toString());
        
        outStream.writeRawByte(ADD_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[0].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[0].toString());
        
        return totalWritten;
    }
    
    CodedInputStream getInStream(int uidSet) throws IOException {
        byte[] value = new byte[1024];
        int totalWritten = 0;
        CodedOutputStream outStream = CodedOutputStream.newInstance(value);
        outStream.writeRawByte(IGNORE);
        totalWritten = 1;
        outStream.writeBoolNoTag(false);
        totalWritten += CodedOutputStream.computeBoolSizeNoTag(false);
        
        outStream.writeRawByte(COUNT);
        totalWritten++;
        outStream.writeUInt64NoTag(4L);
        totalWritten += CodedOutputStream.computeInt64SizeNoTag(4L);
        
        if (uidSet == 1) {
            totalWritten += addUidsSet1(outStream);
        } else if (uidSet == 2) {
            totalWritten += addUidsSet2(outStream);
        } else if (uidSet == 3) {
            totalWritten += addUidsSet3(outStream);
        }
        
        outStream.writeRawByte(REMOVE_UID);
        totalWritten++;
        outStream.writeStringNoTag(uid[3].toString());
        totalWritten += CodedOutputStream.computeStringSizeNoTag(uid[3].toString());
        outStream.writeStringNoTag(uid[4].toString());
        
        // outStream.writeRawByte(DONE);
        CodedInputStream inStream = CodedInputStream.newInstance(value, 0, totalWritten);
        return inStream;
    }
    
    @Test
    public void testUidList() throws IOException {
        
        CodedInputStream inStream = getInStream(1);
        Uid.List ulist = Uid.List.parseFrom(inStream);
        
        assertEquals(ulist.getCOUNT(), 4);
        assertEquals(ulist.getIGNORE(), false);
        
        List<String> uuids = ulist.getUIDList();
        assertEquals(uuids.size(), 3);
        for (int i = 0; i < 3; i++) {
            assertTrue(uuids.contains(uid[i].toString()));
        }
        
        uuids = ulist.getREMOVEDUIDList();
        assertEquals(uuids.size(), 1);
        assertTrue(uuids.contains(uid[3].toString()));
        
        Map<Descriptors.FieldDescriptor,Object> fields = ulist.getAllFields();
    }
    
    @Test
    public void testUidListEqual() throws IOException {
        CodedInputStream inStream = getInStream(1);
        Uid.List ulist1 = Uid.List.parseFrom(inStream);
        
        inStream = getInStream(2);
        Uid.List ulist2 = Uid.List.parseFrom(inStream);
        
        inStream = getInStream(3);
        Uid.List ulist3 = Uid.List.parseFrom(inStream);
        
        boolean isEqual = ulist1.equals(ulist2);
        boolean isTheSame = ulist1.sameAs(ulist2);
        boolean isTheSame2 = ulist1.sameAs2(ulist2);
        
        boolean isEqual_2 = ulist2.equals(ulist3);
        boolean isTheSame_2 = ulist2.sameAs(ulist3);
        boolean isTheSame2_2 = ulist2.sameAs2(ulist3);
    }
    
}
