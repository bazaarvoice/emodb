package com.bazaarvoice.emodb.hive;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.hadoop.io.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Hive Serializer and Deserializer implementation for EmoDB Rows.  Note that it's actually only a Deserializer;
 * all serialization methods are not supported.  However, this isn't an issue because EmoDB tables are always
 * Hive external tables and therefore not writable.
 */
public class EmoSerDe extends AbstractSerDe {

    // Ordered list of column names and types
    private List<Map.Entry<String, TypeInfo>> _columns;
    // List of values deserialized from the last call to deserialize()
    private List<Object> _values;
    // Object inspector for use by Hive
    private ObjectInspector _inspector;

    // Columns that have special meaning if not explicitly found in the row's JSON
    private static enum ImplicitColumn {
        id, table, version, signature, first_update_at, last_update_at, json
    }

    @Override
    public void initialize(Configuration config, Properties properties)
            throws SerDeException {
        // Get the column names and types from the configuration properties
        String columnNamesProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypesProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        List<String> columnNames;
        List<TypeInfo> columnTypes;
        List<ObjectInspector> columnInspectors;

        if (columnNamesProperty.isEmpty()) {
            columnNames = ImmutableList.of();
        } else {
            columnNames = Arrays.asList(columnNamesProperty.split(","));
        }

        if (columnTypesProperty.isEmpty()) {
            columnTypes = ImmutableList.of();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypesProperty);
        }

        int numColumns = columnNames.size();
        checkArgument(columnTypes.size() == numColumns);

        _columns = Lists.newArrayListWithCapacity(numColumns);
        _values = Lists.newArrayListWithCapacity(numColumns);
        columnInspectors = Lists.newArrayListWithCapacity(numColumns);

        // Initialize the types and inspectors for each column
        for (int i=0; i < numColumns; i++) {
            TypeInfo type = columnTypes.get(i);

            ObjectInspector columnInspector = getObjectInspectorForType(type);

            _columns.add(Maps.immutableEntry(columnNames.get(i), type));
            _values.add(null);

            columnInspectors.add(columnInspector);
        }

        _inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnInspectors);
    }

    /**
     * Returns the associated ObjectInspector for a type.  This most delegates the to Hive java implementations but filters
     * out primitives not supported by EmoDB.
     */
    private ObjectInspector getObjectInspectorForType(TypeInfo type)
            throws SerDeException {
        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
                if (isSupportedPrimitive(primitiveType)) {
                    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType.getPrimitiveCategory());
                }
                break;
            case STRUCT:
                StructTypeInfo structType = (StructTypeInfo) type;
                List<ObjectInspector> structInspectors = Lists.newArrayListWithCapacity(structType.getAllStructFieldTypeInfos().size());
                for (TypeInfo fieldType : structType.getAllStructFieldTypeInfos()) {
                    structInspectors.add(getObjectInspectorForType(fieldType));
                }
                return ObjectInspectorFactory.getStandardStructObjectInspector(structType.getAllStructFieldNames(), structInspectors);
            case MAP:
                MapTypeInfo mapType = (MapTypeInfo) type;
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        getObjectInspectorForType(mapType.getMapKeyTypeInfo()), getObjectInspectorForType(mapType.getMapValueTypeInfo()));
            case LIST:
                ListTypeInfo listType = (ListTypeInfo) type;
                return ObjectInspectorFactory.getStandardListObjectInspector(getObjectInspectorForType(listType.getListElementTypeInfo()));
            case UNION:
                UnionTypeInfo unionType = (UnionTypeInfo) type;
                List<ObjectInspector> unionInspectors = Lists.newArrayListWithCapacity(unionType.getAllUnionObjectTypeInfos().size());
                for (TypeInfo fieldType : unionType.getAllUnionObjectTypeInfos()) {
                    unionInspectors.add(getObjectInspectorForType(fieldType));
                }
                return ObjectInspectorFactory.getStandardUnionObjectInspector(unionInspectors);
        }

        // Should be unreachable
        throw new SerDeException("Unsupported type: " + type);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Row.class;
    }

    @Override
    public Object deserialize(Writable writable)
            throws SerDeException {
        Row row = (Row) writable;

        // Since this implementation uses a StructObjectInspector return a list of deserialized values in the same
        // order as the original properties.

        int i = 0;
        for (Map.Entry<String, TypeInfo> column : _columns) {
            String columnName = column.getKey();
            TypeInfo type = column.getValue();

            // Get the raw value from traversing the JSON map
            Object rawValue = getRawValue(columnName, row);
            // Deserialize the value to the expected type
            Object value = deserialize(type, rawValue);

            _values.set(i++, value);
        }

        return _values;
    }

    /**
     * Returns the value for a given row.  Hierarchical elements can be reached using paths like keys.  For example:
     *
     * <code>getRawValue("about/~id")</code>
     *
     * is roughly equivalent to returning:
     *
     * <code>row.getMap().get("about").get("~id")</code>
     *
     * with additional null and type checking along the path.
     *
     * Additionally, most intrinsics can be referenced without the leading tilde, and "json" will return the row as the
     * original JSON string.  Note that preference is always given to an explicit value.  For example, if the row
     * contains a field called "id" then calling this method with column name "id" will return that value, even if it is
     * set to null.  If there is no field called "id" then calling this method with column name "id" will return the
     * intrinsic value for "~id".
     */
    private Object getRawValue(String columnName, Row row) {
        try {
            return getRawValue(columnName, row.getMap());
        } catch (ColumnNotFoundException e) {
            // Check if there is an implicit column override then return it
            try {
                ImplicitColumn implicitColumn = ImplicitColumn.valueOf(columnName.toLowerCase());
                return getImplicitValue(implicitColumn, row);
            } catch (IllegalArgumentException notImplicit) {
                // Object not found and column is not implicit.  Return null.
                return null;
            }
        }
    }

    private Object getImplicitValue(ImplicitColumn field, Row row) {
        switch (field) {
            case id:                return row.getId();
            case table:             return row.getTable();
            case version:           return row.getVersion();
            case signature:         return row.getSignature();
            case first_update_at:   return row.getFirstUpdateAt();
            case last_update_at:    return row.getLastUpdateAt();
            case json:              return row.getJson();
            default:
                // Should be unreachable
                throw new IllegalArgumentException("Unknown implicit field: " + field);
        }
    }

    /**
     * Returns the raw value for a given Map.  If the value was found is and is null then null is returned.  If no
     * value is present then ColumnNotFoundException is thrown.
     * @throws ColumnNotFoundException The column was not found in the map
     */
    private Object getRawValue(String columnName, Map<String, Object> content)
            throws ColumnNotFoundException {
        String field = columnName;
        Object value = content;

        while (field != null) {
            // If at any point in the path a null is encountered stop
            if (value == null) {
                throw new ColumnNotFoundException();
            }

            // With the exception of leaf values the intermediate values must always be Maps.
            if (!(value instanceof Map)) {
                throw new ColumnNotFoundException();
            }

            //noinspection unchecked
            Map<String, Object> map = (Map<String, Object>) value;
            String nextField = null;

            int separator = field.indexOf('/');
            if (separator != -1) {
                nextField = field.substring(separator + 1);
                field = field.substring(0, separator);
            }

            // Typically Hive column names are all lower case.  Because of this we can't just look up the key directly;
            // we need to look it up in a case-insensitive fashion.  For efficiency first try it as-is.

            boolean found = false;
            if (map.containsKey(field)) {
                value = map.get(field);
                found = true;
            } else {
                // Look for the key case-insensitively
                for (Iterator<String> iter = map.keySet().iterator(); !found && iter.hasNext(); ) {
                    String key = iter.next();
                    if (key.equalsIgnoreCase(field)) {
                        value = map.get(key);
                        found = true;
                    }
                }
            }
            if (!found) {
                throw new ColumnNotFoundException();
            }

            field = nextField;
        }

        return value;
    }

    /**
     * Like {@link #getRawValue(String, java.util.Map)} except it returns null if the value is not present.
     */
    private Object getRawValueOrNullIfAbsent(String columnName, Map<String, Object> content)
            throws SerDeException {
        try {
            return getRawValue(columnName, content);
        } catch (ColumnNotFoundException e) {
            return null;
        }
    }

    /**
     * Deserializes a raw value to the provided type.
     */
    private Object deserialize(TypeInfo type, Object rawValue)
            throws SerDeException {
        Object value = null;

        if (rawValue != null) {
            switch (type.getCategory()) {
                case PRIMITIVE:
                    value = deserializePrimitive((PrimitiveTypeInfo) type, rawValue);
                    break;
                case STRUCT:
                    value = deserializeStruct((StructTypeInfo) type, rawValue);
                    break;
                case MAP:
                    value = deserializeMap((MapTypeInfo) type, rawValue);
                    break;
                case LIST:
                    value = deserializeList((ListTypeInfo) type, rawValue);
                    break;
                case UNION:
                    value = deserializeUnion((UnionTypeInfo) type, rawValue);
                    break;
            }
        }

        return value;
    }

    /**
     * Determines if the given primitive is supported by this deserializer.  At this time the only exclusions are
     * BINARY, DECIMAL, VARCHAR, CHAR, and UNKNOWN.
     */
    private boolean isSupportedPrimitive(PrimitiveTypeInfo type) {
        switch (type.getPrimitiveCategory()) {
            case VOID:
            case STRING:
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    /**
     * Deserializes a primitive to its corresponding Java type, doing a best-effort conversion when necessary.
     */
    private Object deserializePrimitive(PrimitiveTypeInfo type, Object value)
            throws SerDeException {
        switch (type.getPrimitiveCategory()) {
            case VOID:
                return null;
            case STRING:
                return deserializeString(value);
            case BOOLEAN:
                return deserializeBoolean(value);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return deserializeNumber(value, type);
            case DATE:
            case TIMESTAMP:
                return deserializeDate(value, type);
            default:
                throw new SerDeException("Unsupported type: " + type.getPrimitiveCategory());
        }
    }

    private Object deserializeString(Object value) {
        if (value instanceof String) {
            return value;
        } else if (value instanceof Map || value instanceof List) {
            // Convert maps and lists back to JSON strings
            return JsonHelper.asJson(value);
        } else {
            // All other types use Java string conversion
            return value.toString();
        }
    }

    private Object deserializeBoolean(Object value) {
        if (value instanceof Boolean) {
            return value;
        } else if (value instanceof Number) {
            return ((Number) value).floatValue() != 0;
        } else {
            return Boolean.valueOf(value.toString());
        }
    }

    private Object deserializeNumber(Object value, PrimitiveTypeInfo type)
            throws SerDeException {
        // Note that only numbers and booleans are supported.  All other types cannot be deserialized.  In particular
        // String representations of numbers are not parsed.
        Number number;
        if (value instanceof Number) {
            number = (Number) value;
        } else if (value instanceof Boolean) {
            number = ((Boolean) value) ? (byte) 1 : 0;
        } else {
            throw new SerDeException("Value is not a " + type + ": " + value);
        }

        switch (type.getPrimitiveCategory()) {
            case BYTE:   return number.byteValue();
            case SHORT:  return number.shortValue();
            case INT:    return number.intValue();
            case LONG:   return number.longValue();
            case FLOAT:  return number.floatValue();
            case DOUBLE: return number.doubleValue();
        }

        throw new SerDeException("Primitive number did not match any expected categories"); // Unreachable
    }

    private Object deserializeDate(Object value, PrimitiveTypeInfo type)
            throws SerDeException {
        long ts;
        // Dates can be either ISO8601 Strings or numeric timestamps.  Any other data type or format cannot be
        // deserialized.
        if (value instanceof String) {
            try {
                ts = JsonHelper.parseTimestamp((String) value).getTime();
            } catch (Exception e) {
                throw new SerDeException("Invalid time string: " + value);
            }
        } else if (value instanceof Number) {
            ts = ((Number) value).longValue();
        } else if (value instanceof java.util.Date) {
            ts = ((java.util.Date) value).getTime();
        } else {
            throw new SerDeException("Invalid time value: " + value);
        }

        if (type.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DATE) {
            return new Date(ts);
        } else {
            return new Timestamp(ts);
        }
    }

    private Object deserializeStruct(StructTypeInfo type, Object data)
            throws SerDeException {
        if (!(data instanceof Map)) {
            throw new SerDeException("Value not of type map");
        }
        //noinspection unchecked
        Map<String, Object> map = (Map<String, Object>) data;

        List<String> fieldNames = type.getAllStructFieldNames();
        List<TypeInfo> fieldTypes = type.getAllStructFieldTypeInfos();

        // When deserializing a struct the returned value is a list of values in the same order as the field names.

        List<Object> values = Lists.newArrayListWithCapacity(fieldNames.size());
        for (int i=0; i < fieldNames.size(); i++) {
            Object rawValue = getRawValueOrNullIfAbsent(fieldNames.get(i), map);
            Object value = deserialize(fieldTypes.get(i), rawValue);
            values.add(value);
        }

        return values;
    }

    private Object deserializeMap(MapTypeInfo type, Object data)
            throws SerDeException {
        if (!(data instanceof Map)) {
            throw new SerDeException("Value not of type map");
        }
        //noinspection unchecked
        Map<String, Object> map = (Map<String, Object>) data;
        Map<Object, Object> values = Maps.newHashMap();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object key = deserialize(type.getMapKeyTypeInfo(), entry.getKey());
            Object value = deserialize(type.getMapValueTypeInfo(), entry.getValue());
            values.put(key, value);
        }

        return values;
    }

    private Object deserializeList(ListTypeInfo type, Object data)
            throws SerDeException {
        if (!(data instanceof List)) {
            throw new SerDeException("Value not of type list");
        }
        //noinspection unchecked
        List<Object> list = (List<Object>) data;

        List<Object> values = Lists.newArrayListWithCapacity(list.size());
        for (Object entry : list) {
            Object value = deserialize(type.getListElementTypeInfo(), entry);
            values.add(value);
        }

        return values;
    }

    private Object deserializeUnion(UnionTypeInfo type, Object data)
            throws SerDeException {
        // Try each union type in order until one matches.
        for (byte i=0; i < type.getAllUnionObjectTypeInfos().size(); i++) {
            try {
                Object value = deserialize(type.getAllUnionObjectTypeInfos().get(i), data);
                return new StandardUnionObjectInspector.StandardUnion(i, value);
            } catch (SerDeException e) {
                // Skip it and try the next
            }
        }

        throw new SerDeException("No suitable type found");
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector)
            throws SerDeException {
        throw new SerDeException("Cannot serialize to Rows");
    }

    @Override
    public ObjectInspector getObjectInspector()
            throws SerDeException {
        return _inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // Common practice is to return null here
        return null;
    }

    /** Exception class used internally when a column is not found. */
    private static class ColumnNotFoundException extends Exception {
        // empty
    }
}
