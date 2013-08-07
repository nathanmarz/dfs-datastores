package com.backtype.support;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;


public class KeyworkArgParserTest extends TestCase {
    private Map<String, Object> getArgs(Object... objs) {
        Map<String, Object> ret = new HashMap<String, Object>();
        for(int i=0; i<objs.length; i+=2) {
            String arg = (String) objs[i];
            Object val = objs[i+1];
            ret.put(arg, val);
        }
        return ret;
    }

    private void assertContains(KeywordArgParser parser, Map<String, Object> args, Map<String,Object> results) {
        Map parsed = parser.parse(args);
        assertEquals(results, parsed);
    }

    private void assertIllegal(KeywordArgParser parser, Object... argsObjs) {
        Map<String, Object> args = getArgs(argsObjs);
        try {
            parser.parse(args);
            fail("Should throw illegal args exception");
        } catch(IllegalArgumentException e) {

        }
    }

    public void testParsing() {
        KeywordArgParser parser = new KeywordArgParser()
                 .add("arg1", "default", false)
                 .add("arg2", null, true, "a", "b")
                 .add("arg3", 5, true, 1, 2, 3, 4, 5);
        assertContains(parser, getArgs("arg1", "lalala"), getArgs("arg1", "lalala", "arg2", null, "arg3", 5));
        assertContains(parser, getArgs("arg2", "b", "arg3", null), getArgs("arg1", "default", "arg2", "b", "arg3", null));
        assertContains(parser, getArgs("arg1", "yo", "arg2", "a", "arg3", 1), getArgs("arg1", "yo", "arg2", "a", "arg3", 1));
        assertIllegal(parser, "arg1", null);
        assertIllegal(parser, "arg4", 1);
        assertIllegal(parser, "arg2", "c");
    }

    public void testInvalidParsers() {
        try {
            new KeywordArgParser()
             .add("a", null, false);
            fail("should fail");
        } catch(IllegalArgumentException e) {}
                try {
            new KeywordArgParser()
             .add("a", "1", false, 1, "2");
            fail("should fail");
        } catch(IllegalArgumentException e) {}
    }
}
