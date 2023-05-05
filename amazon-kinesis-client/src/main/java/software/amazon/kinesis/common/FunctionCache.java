package software.amazon.kinesis.common;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Caches results from a {@link Function}. Caching is especially useful when
 * {@link Function#get(T)} is an expensive call that produces static results.
 */
@RequiredArgsConstructor
public class FunctionCache<T, R> {
    
    private final Function<T, R> function;
    
    private volatile Pair<T,R> result;

    /**
     * Returns the cached result. If the cache is null, the supplier will be
     * invoked to populate the cache.
     *
     * @return cached result which may be null
     */
    public R get(T arg) {
        if (!containsKey(arg)) {
            synchronized (this) {
                // double-check lock
                if (!containsKey(arg)) {
                    result = new ImmutablePair<>(arg, function.apply(arg));
                }
            }
        }
        return result.getValue();
    }
    
    private boolean containsKey(T arg) {
        return result != null && result.getKey().equals(arg) && result.getValue() != null;
    }

}
