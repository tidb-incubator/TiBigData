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
import io.tidb.bigdata.tidb.codec.Codec.RealCodec;
import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.codec.CodecDataOutput;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import org.tikv.common.exception.ConvertNotSupportException;
import org.tikv.common.exception.ConvertOverflowException;
import org.tikv.common.exception.InvalidCodecFormatException;
import org.tikv.common.exception.TypeException;

public class RealType extends DataType {
  public static final RealType DOUBLE = new RealType(MySQLType.TypeDouble);
  public static final RealType FLOAT = new RealType(MySQLType.TypeFloat);
  public static final RealType REAL = DOUBLE;

  public static final MySQLType[] subTypes =
      new MySQLType[] {MySQLType.TypeDouble, MySQLType.TypeFloat};

  private RealType(MySQLType tp) {
    super(tp);
  }

  RealType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == Codec.DECIMAL_FLAG) {
      return DecimalCodec.readDecimal(cdi).doubleValue();
    } else if (flag == Codec.FLOATING_FLAG) {
      return RealCodec.readDouble(cdi);
    }
    throw new InvalidCodecFormatException("Invalid Flag type for float type: " + flag);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToReal(value);
  }

  private Object convertToReal(Object value) throws ConvertNotSupportException {
    Double result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = 1d;
      } else {
        result = 0d;
      }
    } else if (value instanceof Byte) {
      result = ((Byte) value).doubleValue();
    } else if (value instanceof Short) {
      result = ((Short) value).doubleValue();
    } else if (value instanceof Integer) {
      result = ((Integer) value).doubleValue();
    } else if (value instanceof Long) {
      result = ((Long) value).doubleValue();
    } else if (value instanceof Float) {
      result = ((Float) value).doubleValue();
    } else if (value instanceof Double) {
      result = (Double) value;
    } else if (value instanceof String) {
      result = Converter.stringToDouble((String) value);
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }

    if (this.getType() == MySQLType.TypeFloat) {
      return result.floatValue();
    } else {
      return result;
    }
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    double val = Converter.convertToDouble(value);
    RealCodec.writeDoubleFully(cdo, val);
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    double val = Converter.convertToDouble(value);
    RealCodec.writeDoubleFully(cdo, val);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    double val = Converter.convertToDouble(value);
    RealCodec.writeDouble(cdo, val);
  }

  @Override
  public String getName() {
    if (tp == MySQLType.TypeDouble) {
      return "DOUBLE";
    }
    return "FLOAT";
  }

  @Override
  public ExprType getProtoExprType() {
    if (tp == MySQLType.TypeDouble) {
      return ExprType.Float64;
    } else if (tp == MySQLType.TypeFloat) {
      return ExprType.Float32;
    }
    throw new TypeException("Unknown Type encoding proto " + tp);
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return Double.parseDouble(value);
  }
}
