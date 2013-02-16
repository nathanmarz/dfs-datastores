package com.backtype.support;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SubsetSum {

    public interface Value {
        public long getValue();
    }

    private static <T extends Value> long greedy(List<T> input, long targetSize, List<T> ret, List<T> left) {
        long sum = 0;
        for (T p : input) {
            if (sum + p.getValue() <= targetSize) {
                sum += p.getValue();
                ret.add(p);
            } else {
                left.add(p);
            }
        }
        return sum;
    }

    private static <T extends Value> List<T> removeBestSubset(List<T> input, long targetSize) {
        List<T> ret1 = new ArrayList<T>();
        List<T> left1 = new ArrayList<T>();
        List<T> ret2 = new ArrayList<T>();
        List<T> left2 = new ArrayList<T>();

        long size1 = greedy(input, targetSize, ret1, left1);
        Collections.reverse(input);
        long size2 = greedy(input, targetSize, ret2, left2);

        List<T> ret;
        List<T> left;
        if (size1 > size2) {
            ret = ret1;
            left = left1;
        } else {
            ret = ret2;
            left = left2;
        }

        input.clear();
        input.addAll(left);

        return ret;
    }

    //things that are too big get their own set
    public static <T extends Value> List<List<T>> split(List<T> input, long targetSize) {
        Collections.sort(input, new Comparator<Value>() {
            public int compare(Value o1, Value o2) {
                return new Long(o1.getValue()).compareTo(new Long(o2.getValue()));
            }
        });
        List<T> smaller = new ArrayList<T>();
        List<T> bigger = new ArrayList<T>();
        for(T v: input) {
            if(v.getValue() >= targetSize) {
                bigger.add(v);
            } else {
                smaller.add(v);
            }
        }
        List<List<T>> ret = new ArrayList<List<T>>();
        for(T b: bigger) {
            List<T> elem = new ArrayList<T>();
            elem.add(b);
            ret.add(elem);
        }

        while(smaller.size()>0) {
            ret.add(removeBestSubset(smaller, targetSize));
        }
        return ret;
    }
}
