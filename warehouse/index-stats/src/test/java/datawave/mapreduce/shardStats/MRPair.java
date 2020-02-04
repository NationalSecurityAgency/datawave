package datawave.mapreduce.shardStats;

class MRPair<KEY,VALUE> {
    
    KEY key;
    VALUE value;
    
    MRPair(KEY k, VALUE v) {
        this.key = k;
        this.value = v;
    }
}
