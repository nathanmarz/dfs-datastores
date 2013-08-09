package com.backtype.hadoop.pail;

import java.util.ArrayList;
import java.util.List;

public class TestStructure implements PailStructure<String> {

    public boolean isValidTarget(String... dirs) {
        if (dirs.length == 0) {
            return false;
        }
        String first = dirs[0];
        if (first.length() != 1) {
            return false;
        }
        if (!first.equals("z")) {
            return true;
        }
        if (dirs.length < 2) {
            return false;
        }
        return dirs[1].length() == 1;
    }

    public String deserialize(byte[] serialized) {
        return new String(serialized);
    }

    public byte[] serialize(String object) {
        return object.getBytes();
    }

    public List<String> getTarget(String object) {
        List<String> target = new ArrayList<String>();
        if (object.length() == 0) {
            target.add("a");
        } else {
            String f1 = object.substring(0, 1);
            target.add(f1);
            if (f1.equals("z")) {
                target.add(object.substring(1, 2));
            }
        }
        return target;
    }

    public Class getType() {
        return String.class;
    }
}
