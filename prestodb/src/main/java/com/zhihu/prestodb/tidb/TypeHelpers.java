/*
 * Copyright 2020 Zhihu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zhihu.prestodb.tidb;

import com.facebook.presto.spi.type.*;
import com.pingcap.tikv.types.*;
import com.zhihu.presto.tidb.RecordCursorInternal;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import io.airlift.slice.Slice;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.math.BigDecimal;
import java.math.MathContext;

import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeShortScaledValue;
import static com.zhihu.prestodb.tidb.TypeHelper.*;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

final public class TypeHelpers
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private static final ConcurrentHashMap<DataType, Optional<TypeHelper>> TYPE_HELPERS = new ConcurrentHashMap<>();

    public static TypeHelper decimalHelper(DataType tidbType, com.facebook.presto.spi.type.DecimalType decimalType)
    {
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return longHelper(tidbType, decimalType, (cursor, columnIndex) -> encodeShortScaledValue(cursor.getBigDecimal(columnIndex), scale));
        }
        return sliceHelper(tidbType, decimalType,
                (cursor, columnIndex) -> encodeScaledValue(cursor.getBigDecimal(columnIndex), scale),
                s -> new BigDecimal(decodeUnscaledValue(s), scale, new MathContext(decimalType.getPrecision())));
    }

    public static TypeHelper varcharHelper(DataType tidbType, VarcharType varcharType)
    {
        return sliceHelper(tidbType, varcharType, (cursor, columnIndex) -> utf8Slice(cursor.getString(columnIndex)));
    }

    private static TypeHelper getHelperInternal(DataType type)
    {
        long length = type.getLength();
        switch (type.getType()) {
            case TypeTiny:
                // FALLTHROUGH
            case TypeBit:
                return longHelper(type, com.facebook.presto.spi.type.TinyintType.TINYINT, RecordCursorInternal::getByte);
            case TypeYear:
            case TypeShort:
                return longHelper(type, com.facebook.presto.spi.type.SmallintType.SMALLINT, RecordCursorInternal::getShort);
            case TypeInt24:
                // FALLTHROUGH
            case TypeLong:
                return longHelper(type, com.facebook.presto.spi.type.IntegerType.INTEGER, RecordCursorInternal::getInteger);
            case TypeFloat:
                return longHelper(type, com.facebook.presto.spi.type.RealType.REAL,
                        (cursor, column) -> floatToRawIntBits(cursor.getFloat(column)),
                        l -> intBitsToFloat(l.intValue()));
            case TypeDouble:
                return doubleHelper(type, com.facebook.presto.spi.type.DoubleType.DOUBLE, RecordCursorInternal::getDouble);
            case TypeNull:
                return null;
            case TypeDatetime:
                // FALLTHROUGH
            case TypeTimestamp:
                return longHelper(type, com.facebook.presto.spi.type.TimestampType.TIMESTAMP,
                        (cursor, columnIndex) -> cursor.getTimestamp(columnIndex).getTime(),
                        Timestamp::new);
            case TypeLonglong:
                return longHelper(type, com.facebook.presto.spi.type.BigintType.BIGINT, RecordCursorInternal::getLong);
            case TypeDate:
                // FALLTHROUGH
            case TypeNewDate:
                return longHelper(type, com.facebook.presto.spi.type.DateType.DATE, (cursor, columnIndex) -> {
                    long localMillis = cursor.getDate(columnIndex).getTime();
                    DateTimeZone zone = ISOChronology.getInstance().getZone();
                    return MILLISECONDS.toDays(zone.getMillisKeepLocal(UTC, localMillis));
                }, days -> new Date(DAYS.toMillis((long) days)));
            case TypeDuration:
                return longHelper(type, com.facebook.presto.spi.type.TimeType.TIME, (cursor, columnIndex) -> {
                    long localMillis = cursor.getLong(columnIndex) / 1000000L;
                    DateTimeZone zone = ISOChronology.getInstance().getZone();
                    return zone.convertUTCToLocal(zone.getMillisKeepLocal(UTC, localMillis));
                }, millis -> (1000000L * (((long) millis) + 8 * 3600 * 1000L)));
            case TypeJSON:
                return sliceHelper(type, com.facebook.presto.spi.type.JsonType.JSON, (cursor, columnIndex) -> utf8Slice(cursor.getString(columnIndex)));
            case TypeSet:
                // TiKV client might has issue related to set, disable it at this time.
                return null;
            case TypeTinyBlob:
                // FALLTHROUGH
            case TypeMediumBlob:
                // FALLTHROUGH
            case TypeLongBlob:
                // FALLTHROUGH
            case TypeBlob:
                // FALLTHROUGH
            case TypeEnum:
                // FALLTHROUGH
            case TypeVarString:
                // FALLTHROUGH
            case TypeString:
                // FALLTHROUGH
            case TypeVarchar:
                // FALLTHROUGH
                if (type instanceof StringType || type instanceof SetType || type instanceof EnumType) {
                    if (length > (long) VarcharType.MAX_LENGTH || length < 0) {
                        return varcharHelper(type, VarcharType.createUnboundedVarcharType());
                    }
                    return varcharHelper(type, VarcharType.createVarcharType((int) length));
                }
                else if (type instanceof BytesType) {
                    return sliceHelper(type, com.facebook.presto.spi.type.VarbinaryType.VARBINARY,
                            (cursor, columnIndex) -> wrappedBuffer(cursor.getBytes(columnIndex)), Slice::getBytes);
                }
                else {
                    return null;
                }
            case TypeDecimal:
                // FALLTHROUGH
            case TypeNewDecimal:
                int decimalDigits = type.getDecimal();
                int precision = (int) length + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return null;
                }
                return decimalHelper(type, com.facebook.presto.spi.type.DecimalType.createDecimalType(precision, max(decimalDigits, 0)));
            case TypeGeometry:
                // FALLTHROUGH
            default:
                return null;
        }
    }

    public static Optional<TypeHelper> getHelper(DataType type)
    {
        Optional<TypeHelper> helper = TYPE_HELPERS.get(type);
        if (helper != null) {
            return helper;
        }
        helper = Optional.ofNullable(getHelperInternal(type));
        TYPE_HELPERS.putIfAbsent(type, helper);
        return helper;
    }

    public static Optional<Type> getPrestoType(DataType type)
    {
        return getHelper(type).map(TypeHelper::getPrestoType);
    }
}
