/*
 *
 * Copyright 2017 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.types;

import com.pingcap.tidb.tipb.ExprType;
import io.tidb.bigdata.tidb.codec.Codec;
import io.tidb.bigdata.tidb.codec.Codec.DecimalCodec;
import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.codec.CodecDataOutput;
import io.tidb.bigdata.tidb.codec.MyDecimal;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.tikv.common.exception.ConvertNotSupportException;
import org.tikv.common.exception.ConvertOverflowException;
import org.tikv.common.exception.InvalidCodecFormatException;

public class DecimalType extends DataType {
  public static final DecimalType DECIMAL = new DecimalType(MySQLType.TypeNewDecimal);
  public static final DecimalType BIG_INT_DECIMAL = new DecimalType(38, 0);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeNewDecimal};

  private DecimalType(MySQLType tp) {
    super(tp);
  }

  public DecimalType(int prec, int scale) {
    super(MySQLType.TypeNewDecimal, prec, scale);
  }

  DecimalType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.DECIMAL_FLAG) {
      throw new InvalidCodecFormatException("Invalid Flag type for decimal type: " + flag);
    }
    return DecimalCodec.readDecimal(cdi);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToMysqlDecimal(value);
  }

  private MyDecimal convertToMysqlDecimal(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    BigDecimal result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = BigDecimal.ONE;
      } else {
        result = BigDecimal.ZERO;
      }
    } else if (value instanceof Byte) {
      result = BigDecimal.valueOf(((Byte) value).longValue());
    } else if (value instanceof Short) {
      result = BigDecimal.valueOf(((Short) value).longValue());
    } else if (value instanceof Integer) {
      result = BigDecimal.valueOf(((Integer) value).longValue());
    } else if (value instanceof Long) {
      result = BigDecimal.valueOf((Long) value);
    } else if (value instanceof Float) {
      result = BigDecimal.valueOf(((Float) value).doubleValue());
    } else if (value instanceof Double) {
      result = BigDecimal.valueOf((Double) value);
    } else if (value instanceof String) {
      result = new BigDecimal((String) value);
    } else if (value instanceof BigDecimal) {
      result = (BigDecimal) value;
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }

    int precision = (int) this.getLength(); // -> scale
    int frac = this.getDecimal(); // -> precision
    return toGivenPrecisionAndFrac(result, precision, frac);
  }

  /**
   * convert a BigDecimal to a MyDecimal according to a give precision and frac, e.g.
   * toGivenPrecisionAndFrac(1.234, 3, 2) = 1.23 toGivenPrecisionAndFrac(1.235, 3, 2) = 1.25
   * toGivenPrecisionAndFrac(1.235, 5, 4) = 1.2350 toGivenPrecisionAndFrac(11.235, 4, 3) throw
   * ConvertOverflowException
   *
   * @param value
   * @param precision
   * @param frac
   * @return
   * @throws ConvertOverflowException
   */
  private MyDecimal toGivenPrecisionAndFrac(BigDecimal value, int precision, int frac)
      throws ConvertOverflowException {
    BigDecimal roundedValue = value.setScale(frac, RoundingMode.HALF_UP);

    if (roundedValue.precision() > precision) {
      throw ConvertOverflowException.newOutOfRange();
    }

    MyDecimal roundedMyDecimal = new MyDecimal();
    roundedMyDecimal.fromString(roundedValue.toPlainString());
    int[] bin = roundedMyDecimal.toBin(precision, frac);

    MyDecimal resultMyDecimal = new MyDecimal();
    resultMyDecimal.fromBin(precision, frac, bin);

    return resultMyDecimal;
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    if (value instanceof BigDecimal) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(((BigDecimal) value).toPlainString());
      DecimalCodec.writeDecimalFully(cdo, dec, (int) this.length, this.decimal);
    } else {
      DecimalCodec.writeDecimalFully(cdo, (MyDecimal) value, (int) this.length, this.decimal);
    }
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    // we can simply encodeKey here since the encoded value of decimal is also need comparable.
    encodeKey(cdo, value);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    MyDecimal dec = new MyDecimal();
    dec.fromString(val.toPlainString());
    DecimalCodec.writeDecimal(cdo, dec, dec.precision(), dec.frac());
  }

  @Override
  public String getName() {
    return String.format("DECIMAL(%d, %d)", length, decimal);
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlDecimal;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return new BigDecimal(value);
  }
}
