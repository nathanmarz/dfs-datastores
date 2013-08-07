/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.backtype.support;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KeywordArgParser {
    private Map<String, Object> _expected = new HashMap<String, Object>();
    private Map<String, Set<Object>> _expectedAllowed = new HashMap<String, Set<Object>>();
    private Map<String, Boolean> _expectedNullable = new HashMap<String, Boolean>();

    public KeywordArgParser add(String arg, Object def, boolean allowNull, Object... allowedVals) {
        if(_expected.containsKey(arg)) throw new IllegalArgumentException(arg + " already within parser");
        _expected.put(arg, def);
        Set<Object> allowed = new HashSet<Object>(Arrays.asList(allowedVals));
        _expectedAllowed.put(arg, allowed);
        _expectedNullable.put(arg, allowNull);
        checkAllowed(arg, def);
        return this;
    }

    private void checkAllowed(String arg, Object val) {
        if(!_expected.containsKey(arg)) {
            throw new IllegalArgumentException("Invalid argument " + arg);
        }
        Set<Object> set = _expectedAllowed.get(arg);
        boolean nullable = _expectedNullable.get(arg);
        if(val==null && !nullable || val!=null && set.size()>0 && !set.contains(val)) {
            throw new IllegalArgumentException(val + " not a legal value");
        }
    }

    public Map<String, Object> parse(Map<String, Object> args) {
        Map<String, Object> ret = new HashMap<String, Object>();
        for(String key: args.keySet()) {
            Object val = args.get(key);
            checkAllowed(key, val);
            ret.put(key, val);
        }
        for(String key: _expected.keySet()) {
            if(!ret.containsKey(key)) {
                ret.put(key, _expected.get(key));
            }
        }
        return ret;
    }
}
