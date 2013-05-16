/**
 * 
 */
package com.backtype.cascading.tap;

import java.io.Serializable;
import java.util.List;

import cascading.tuple.Fields;

import com.backtype.hadoop.pail.PailPathLister;
import com.backtype.hadoop.pail.PailSpec;

public class PailTapOptions implements Serializable {
    public PailSpec getSpec() {
        return spec;
    }

    public Fields getOutputFields() {
        return outputFields;
    }

    public List<String>[] getAttrs() {
        return attrs;
    }

    public PailPathLister getLister() {
        return lister;
    }

    PailSpec spec = null;
    Fields outputFields = new Fields( "bytes");
    List<String>[] attrs = null;
    PailPathLister lister = null;

    public PailTapOptions() {

    }

    public PailTapOptions spec(PailSpec spec) {
        this.spec = spec;
        return this;
    }

    public PailTapOptions outputFields(Fields outputFields) {
        this.outputFields = outputFields;
        return this;
    }

    public PailTapOptions attrs(List<String>[] attrs) {
        this.attrs = attrs;
        return this;
    }

    public PailTapOptions lister(PailPathLister lister) {
        this.lister = lister;
        return this;
    }

}