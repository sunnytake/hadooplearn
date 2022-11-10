package com.msb.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TSortComparator extends WritableComparator {

    public TSortComparator() {
        super(TKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TKey key1 = (TKey) a;
        TKey key2 = (TKey) b;

        int res1 = Integer.compare(key1.getYear(), key2.getYear());
        if(res1 == 0){
            int res2 = Integer.compare(key1.getMonth(), key2.getMonth());
            if(res2 == 0){
                return Integer.compare(key2.getTemperature(), key1.getTemperature());
            }
            return res2;
        }
        return res1;
    }
}
