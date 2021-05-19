package s3up;

import java.util.concurrent.Callable;

public class Util {
    /**
     * A little utility to squash checked exceptions into RuntimeExceptions.
     *
     * @param <T> A callable
     * @param call a call
     * @return identity without checked exceptions
     */
    public static <T> T doesNotThrow(Callable<T> call) {
        try {
            return call.call();
        } catch (Exception t) {
            throw new RuntimeException(t);
        }
    }
}
