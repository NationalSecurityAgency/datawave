package datawave.query.tables;

import java.util.BitSet;
import java.util.Iterator;

public class BitSetIterator implements Iterator<Integer> {

    private int index;
    private BitSet bitset;

    public BitSetIterator(BitSet bitset) {
        this.bitset = bitset;
        this.index = 0;
    }

    public void reset(BitSet bitset) {
        this.bitset = bitset;
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return bitset != null && bitset.nextSetBit(index) >= 0;
    }

    @Override
    public Integer next() {
        int next = bitset.nextSetBit(index);
        index = next + 1;
        return next;
    }
}
