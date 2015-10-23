package com.backtype.support;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class RetryTest extends TestCase {

    public void testRetryTimes() {
        final List<String> retry = new ArrayList<String>();

        boolean result = Retry.retry(3, null, new Function<Object, Boolean>() {
            @Override
            public Boolean apply(Object obj) {
                try {
                    int x = 1 / 0;
                    return true;
                } catch (RuntimeException e) {
                    retry.add("Retrying");
                }
                return false;
            }
        }, new Predicate<Boolean>() {
            @Override
            public boolean apply(Boolean aBoolean) {
                return aBoolean;
            }
        });

        assertEquals(3, retry.size());
        assertEquals(false, result);
    }

}
