package com.bazaarvoice.emodb.common.json;

import com.bazaarvoice.emodb.common.json.deferred.LazyJsonMap;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class JsonValidator {

    public static Object checkValid(@Nullable Object obj) {
        if (obj == null || obj instanceof String || obj instanceof Boolean) {
            // ok!
        } else if (obj instanceof Number) {
            if (!isSupportedNumberType(obj)) {
                throw new JsonValidationException("Unsupported JSON numeric type: " + obj + " (class=" + obj.getClass().getName() + ")");
            }
        } else if (obj instanceof List) {
            for (Object value : ((List<?>) obj)) {
                checkValid(value);
            }
        } else if (obj instanceof LazyJsonMap) {
            LazyJsonMap lazyJsonMap = (LazyJsonMap) obj;
            if (lazyJsonMap.isDeserialized()) {
                // Map is already deserialized so it can be validated as a whole with no deserialization cost.
                validateMap(lazyJsonMap);
            } else {
                // The serialized JSON string contained in the instance should already be valid.
                // For efficiency only validate any attributes on top of the original JSON.  Otherwise, validating each
                // entry in the map could cause an unnecessary and expensive deserialization.
                Map<String, Object> overrides = lazyJsonMap.getOverrides();
                if (overrides != null) {
                    validateMap(overrides);
                }
            }
        } else if (obj instanceof Map) {
            validateMap((Map<?, ?>) obj);
        } else {
            throw new JsonValidationException("Unsupported JSON value type: " + obj + " (class=" + obj.getClass().getName() + ")");
        }
        return obj;
    }

    private static void validateMap(Map<?,?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            if (!(key instanceof String)) {
                throw new JsonValidationException("JSON map keys must be instances of String: " + key + " (class=" + (key != null ? key.getClass().getName() : "null") + ")");
            }
            checkValid(entry.getValue());
        }
    }

    private static boolean isSupportedNumberType(Object obj) {
        if (obj instanceof Integer || obj instanceof Long || obj instanceof Byte || obj instanceof Short) {
            return true;
        }
        if (obj instanceof Double) {
            Double num = (Double) obj;
            return !num.isInfinite() && !num.isNaN();
        }
        if (obj instanceof Float) {
            Float num = (Float) obj;
            return !num.isInfinite() && !num.isNaN();
        }
        if (obj instanceof BigInteger) {
            // The parser supports up to 64-bit Longs then it switches to Double.
            BigInteger num = (BigInteger) obj;
            return obj.equals(BigInteger.valueOf(num.longValue())) || !Double.isInfinite(num.doubleValue());
        }
        if (obj instanceof BigDecimal) {
            // Big decimal doesn't support infinity or nan so no problems there.  We will round it to double precision though.
            return false;  // todo: we could support some BigDecimal values, but it's tricky to determine which ones will overflow/underflow.
        }
        return false;
    }
}
