package io.bootique.kafka.client;

import java.util.Map;

/**
 * @since 0.2
 */
public class FactoryUtils {

    public static void setRequiredProperty(Map<String, Object> map, String key, Object... valueChoices) {
        Object v = firstNonNull(valueChoices);
        if (v == null) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }

        map.put(key, v);
    }

    public static void setProperty(Map<String, Object> map, String key, Object... valueChoices) {

        Object v = firstNonNull(valueChoices);
        if (v != null) {
            map.put(key, v);
        }
    }

    static Object firstNonNull(Object... values) {
        if (values == null) {
            return null;
        }

        for (Object v : values) {
            if (v != null) {
                return v;
            }
        }

        return null;
    }
}
