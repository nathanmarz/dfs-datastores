/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.backtype.support;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FSTestCase extends TestCase {
    public FileSystem local;
    public FileSystem fs;

    public FSTestCase() {
        try {
            local = FileSystem.getLocal(new Configuration());
            fs = FileSystem.get(new Configuration());
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
