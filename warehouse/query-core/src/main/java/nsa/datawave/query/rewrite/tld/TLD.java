package nsa.datawave.query.rewrite.tld;

import java.util.ArrayList;

import nsa.datawave.data.hash.UIDConstants;
import nsa.datawave.query.rewrite.Constants;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

public class TLD {
    
    private TLD() {}
    
    /**
     * - the second null byte is the beginning of the hash - the hash will have 2 dots. all subsequent dots denote a child - therefore, we need to return the
     * subsequence at (firstNull, 3rd dot) or (firstNull, end of sequence) if is no child document
     * 
     * @param qualifier
     * @return
     */
    public static ByteSequence parsePointerFromFI(ByteSequence qualifier) {
        ArrayList<Integer> deezNulls = lastInstancesOf(0, qualifier, 2);
        final int start = deezNulls.get(1) + 1, stop = qualifier.length();
        return qualifier.subSequence(start, stop);
    }
    
    public static ByteSequence parseFieldAndValueFromFI(ByteSequence cf, ByteSequence cq) {
        ArrayList<Integer> deezNulls = instancesOf(0, cf, 1);
        final int startFn = deezNulls.get(0) + 1, stopFn = cf.length();
        
        deezNulls = lastInstancesOf(0, cq, 2);
        final int startFv = 0, stopFv = deezNulls.get(1);
        
        byte[] fnFv = new byte[stopFn - startFn + 1 + stopFv - startFv];
        
        System.arraycopy(cf.getBackingArray(), startFn + cf.offset(), fnFv, 0, stopFn - startFn);
        System.arraycopy(cq.getBackingArray(), startFv + cq.offset(), fnFv, stopFn - startFn + 1, stopFv - startFv);
        return new ArrayByteSequence(fnFv);
    }
    
    public static ByteSequence parseFieldAndValueFromTF(ByteSequence cq) {
        ArrayList<Integer> deezNulls = instancesOf(0, cq, 2);
        ArrayList<Integer> deezOtherNulls = lastInstancesOf(0, cq, 1);
        final int startFn = deezOtherNulls.get(0) + 1, stopFn = cq.length();
        final int startFv = deezNulls.get(1) + 1, stopFv = deezOtherNulls.get(0);
        
        byte[] fnFv = new byte[stopFn - startFn + 1 + stopFv - startFv];
        
        System.arraycopy(cq.getBackingArray(), startFn + cq.offset(), fnFv, 0, stopFn - startFn);
        System.arraycopy(cq.getBackingArray(), startFv + cq.offset(), fnFv, stopFn - startFn + 1, stopFv - startFv);
        return new ArrayByteSequence(fnFv);
    }
    
    /**
     * - * - the second null byte is the beginning of the hash - the hash will have - * 2 dots. all subsequent dots denote a child - therefore, we need to
     * return - * the subsequence at (firstNull, 3rd dot) or (firstNull, end of sequence) - * if is no child document - * @param qualifier - * @return -
     */
    public static ByteSequence parseRootPointerFromFI(ByteSequence qualifier) {
        ArrayList<Integer> deezNulls = lastInstancesOf(0, qualifier, 2);
        final int start = deezNulls.get(1) + 1, stop = qualifier.length();
        return parseRootPointerFromId(qualifier.subSequence(start, stop));
    }
    
    /**
     * parses out of any id
     * 
     * @param id
     * @return
     */
    public static ByteSequence parseRootPointerFromId(ByteSequence id) {
        ArrayList<Integer> deezDots = instancesOf('.', id);
        int stop;
        if (deezDots.size() > 2) {
            stop = deezDots.get(2);
        } else {
            stop = id.length();
        }
        return id.subSequence(0, stop);
    }
    
    /**
     * parses out of any id
     * 
     * @param id
     * @return
     */
    public static ByteSequence estimateRootPointerFromId(ByteSequence id) {
        if (id.length() > 21) {
            for (int i = 21; i < id.length(); ++i) {
                if (id.byteAt(i) == '.') {
                    return id.subSequence(0, i);
                }
            }
            
            return id.subSequence(0, id.length());
        } else {
            return parseParentPointerFromId(id);
        }
        
    }
    
    public static boolean isRootPointer(ByteSequence id) {
        ArrayList<Integer> deezDots = instancesOf('.', id);
        return deezDots.size() <= 2;
    }
    
    public static ByteSequence fromString(String s) {
        return new ArrayByteSequence(s.getBytes());
    }
    
    public static String parseRootPointerFromId(String id) {
        int index = 0;
        for (int i = 0; i < 3; i++) {
            index = id.indexOf(".", index);
            if (index == -1) {
                return id;
            }
            index += 1;
        }
        
        return id.substring(0, index - 1);
    }
    
    public static ByteSequence parseParentPointerFromId(ByteSequence id) {
        ArrayList<Integer> deezDots = instancesOf('.', id);
        int stop;
        if (deezDots.size() > 2) {
            stop = deezDots.get(Math.max(2, deezDots.size() - 2));
        } else {
            stop = id.length();
        }
        return id.subSequence(0, stop);
    }
    
    public static ArrayList<Integer> instancesOf(int b, ByteSequence sequence) {
        return instancesOf(b, sequence, -1);
    }
    
    public static ArrayList<Integer> instancesOf(int b, ByteSequence sequence, int repeat) {
        ArrayList<Integer> positions = Lists.newArrayList();
        for (int i = 0; i < sequence.length() && positions.size() != repeat; ++i) {
            if (sequence.byteAt(i) == b) {
                positions.add(i);
            }
        }
        return positions;
    }
    
    public static ArrayList<Integer> lastInstancesOf(int b, ByteSequence sequence, int repeat) {
        ArrayList<Integer> positions = Lists.newArrayList();
        for (int i = sequence.length() - 1; i >= 0 && positions.size() != repeat; --i) {
            if (sequence.byteAt(i) == b) {
                positions.add(i);
            }
        }
        return positions;
    }
    
    public static Key buildParentKey(Text shard, ByteSequence id, ByteSequence cq, Text cv, long ts) {
        Text ptr = new Text();
        ptr.set(id.getBackingArray(), id.offset(), id.length());
        Text cqt = new Text();
        cqt.set(cq.getBackingArray(), cq.offset(), cq.length());
        return new Key(shard, ptr, cqt, cv, ts);
    }
    
    public static Key getNextParentKey(Key docKey) {
        Text startColfam = docKey.getColumnFamily();
        if (startColfam.find(Constants.NULL) != -1) {
            // we have a start key with a document uid, add to the end of the cf to ensure we go to the next doc
            // parse out the uid
            String cf = startColfam.toString();
            int index = cf.indexOf('\0');
            if (index >= 0) {
                String uid = cf.substring(index + 1);
                int index2 = uid.indexOf('\0');
                if (index2 >= 0) {
                    uid = uid.substring(0, index2);
                }
                // if we do not have an empty uid
                if (!uid.isEmpty()) {
                    uid = TLD.parseRootPointerFromId(uid);
                    // to get to the next doc, add the separator for the UID "extra" (child doc) portion and then the max unicode string
                    Text nextDoc = new Text(cf.substring(0, index) + Constants.NULL + uid + UIDConstants.DEFAULT_SEPARATOR + Constants.MAX_UNICODE_STRING);
                    docKey = new Key(docKey.getRow(), nextDoc, docKey.getColumnQualifier(), docKey.getColumnVisibility(), docKey.getTimestamp());
                }
            }
        }
        return docKey;
    }
}
