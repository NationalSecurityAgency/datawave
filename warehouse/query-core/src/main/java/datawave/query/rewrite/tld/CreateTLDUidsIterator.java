package datawave.query.rewrite.tld;

import java.util.Set;

import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.IndexMatch;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.collect.Sets;

public class CreateTLDUidsIterator extends CreateUidsIterator {
    
    @Override
    public IndexInfo getValue() {
        IndexInfo indexInfo = super.getValue();
        if (indexInfo.onlyEvents()) {
            Set<IndexMatch> uids = indexInfo.uids();
            Set<IndexMatch> parentUids = Sets.newHashSetWithExpectedSize(uids.size());
            for (IndexMatch uid : uids) {
                parentUids.add(new IndexMatch(TLD.parseRootPointerFromId(uid.getUid()), uid.getNode()));
            }
            return new IndexInfo(parentUids);
        } else {
            return indexInfo;
        }
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CreateTLDUidsIterator itr = new CreateTLDUidsIterator();
        itr.src = src.deepCopy(env);
        return itr;
    }
    
}
