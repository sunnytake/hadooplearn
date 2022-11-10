package com.msb.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupingComparator extends WritableComparator {

    public TGroupingComparator() {
        super(TKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TKey key1 = (TKey) a;
        TKey key2 = (TKey) b;
        int res1 = Integer.compare(key1.getYear(), key2.getYear());
        if(res1 == 0){
            return Integer.compare(key1.getMonth(), key2.getMonth());
        }
        return res1;
    }
}
