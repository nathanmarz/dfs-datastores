package com.backtype.support;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

public class Retry {

    public static <P, Q> Q retry(int maxRetries, P argument, Function<P, Q> action, Predicate<Q> isSuccess) {
        int retries = 1;
        Q result = null;
        while (retries <= maxRetries) {
            result = action.apply(argument);
            if (isSuccess.apply(result))
                return result;
            try {
                Thread.sleep(1000 * retries);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            retries++;
        }
        return result;
    }
}
