package com.bazaarvoice.emodb.common.json;

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
        } else if (obj instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                Object key = entry.getKey();
                if (!(key instanceof String)) {
                    throw new JsonValidationException("JSON map keys must be instances of String: " + key + " (class=" + (key != null ? key.getClass().getName() : "null") + ")");
                }
                checkValid(entry.getValue());
            }
        } else {
            throw new JsonValidationException("Unsupported JSON value type: " + obj + " (class=" + obj.getClass().getName() + ")");
        }
        return obj;
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
