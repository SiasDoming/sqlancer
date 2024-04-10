package sqlancer.oushudb.ast;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;

public abstract class OushuDBConstant implements OushuDBExpression {

    public static class SmallintConstant extends OushuDBConstant {

        private final short val;

        public SmallintConstant(short val) {
            this.val = val;
        }

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.SMALLINT;
        }

        @Override
        public short getShort() {
            return val;
        }

        @Override
        public Object getValue() {
            return val;
        }

        @Override
        public String getTextRepresentation() {
            return Short.toString(val);
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return Short.toString(val);
        }

        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            switch (dataType) {
            case SMALLINT:
                return this;
            case INTEGER:
                return new IntegerConstant((int) val);
            case BIGINT:
                return new BigintConstant((long) val);
            case CHAR:
            case VARCHAR:
            case TEXT:
                return new StringConstant(Short.toString(val));
            case BOOLEAN:
                return val != 0 ? OushuDBConstant.trueConstant : OushuDBConstant.falseConstant;
            default:
                throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
            }
        }

    }

    public static class IntegerConstant extends OushuDBConstant {

        private final int val;

        public IntegerConstant(int val) {
            this.val = val;
        }

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.INTEGER;
        }

        @Override
        public int getInt() {
            return val;
        }

        @Override
        public Object getValue() {
            return val;
        }

        @Override
        public String getTextRepresentation() {
            return Integer.toString(val);
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return Integer.toString(val);
        }

        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            switch (dataType) {
            case SMALLINT:
                if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
                    return new SmallintConstant((short) val);
                } else {
                    throw new IllegalArgumentException("smallint out of range");
                }
            case INTEGER:
                return this;
            case BIGINT:
                return new BigintConstant((long) val);
            case CHAR:
            case VARCHAR:
            case TEXT:
                return new StringConstant(Integer.toString(val));
            case BOOLEAN:
                return val != 0 ? OushuDBConstant.trueConstant : OushuDBConstant.falseConstant;
            default:
                throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
            }
        }

    }

    public static class BigintConstant extends OushuDBConstant {

        private final long val;

        public BigintConstant(long val) {
            this.val = val;
        }

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.BIGINT;
        }

        @Override
        public long getLong() {
            return val;
        }

        @Override
        public Object getValue() {
            return val;
        }

        @Override
        public String getTextRepresentation() {
            return Long.toString(val);
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return Long.toString(val);
        }

        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            switch (dataType) {
            case SMALLINT:
                if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
                    return new SmallintConstant((short) val);
                } else {
                    throw new IllegalArgumentException("smallint out of range");
                }
            case INTEGER:
                if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                    return new IntegerConstant((int) val);
                } else {
                    throw new IllegalArgumentException("integer out of range");
                }
            case BIGINT:
                return this;
            case CHAR:
            case VARCHAR:
            case TEXT:
                return new StringConstant(Long.toString(val));
            case BOOLEAN:
                return val != 0L ? OushuDBConstant.trueConstant : OushuDBConstant.falseConstant;
            default:
                throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
            }
        }

    }

    public static class NumericConstant extends OushuDBConstant {

        private final BigDecimal val;

        public NumericConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.NUMERIC;
        }

        @Override
        public BigDecimal getBigDecimal() {
            return val;
        }

        @Override
        public Object getValue() {
            return val;
        }

        @Override
        public String getTextRepresentation() {
            return val.toPlainString();
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return val.toPlainString();
        }

        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            BigInteger bigIntegerVal = val.setScale(0, RoundingMode.HALF_UP).toBigInteger();
            switch (dataType) {
            case SMALLINT:
                try {
                    return new SmallintConstant(bigIntegerVal.shortValueExact());
                } catch (ArithmeticException e) {
                    throw new IllegalArgumentException("smallint out of range");
                }
            case INTEGER:
                try {
                    return new IntegerConstant(bigIntegerVal.intValueExact());
                } catch (ArithmeticException e) {
                    throw new IllegalArgumentException("integer out of range");
                }
            case BIGINT:
                try {
                    return new BigintConstant(bigIntegerVal.longValueExact());
                } catch (ArithmeticException e) {
                    throw new IllegalArgumentException("bigint out of range");
                }
            case CHAR:
            case VARCHAR:
            case TEXT:
                // FIXME: make casting scale-aware by 0-padding to the specified scale
                return new StringConstant(val.toPlainString());
            case BOOLEAN:
            default:
                throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
            }
        }

    }

    public static class StringConstant extends OushuDBConstant {

        private final String val;

        public StringConstant(String val) {
            this.val = val;
        }

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.TEXT;
        }

        @Override
        public String getString() {
            return val;
        }

        @Override
        public Object getValue() {
            return val;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", val.replace("'", "''"));
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return val;
        }

        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            switch (dataType) {
            case SMALLINT:
                return new SmallintConstant(Short.parseShort(val, 10));
            case INTEGER:
                return new IntegerConstant(Integer.parseInt(val, 10));
            case BIGINT:
                return new BigintConstant(Long.parseLong(val, 10));
            case CHAR:
            case VARCHAR:
            case TEXT:
                return this;
            case BOOLEAN:
                throw new UnsupportedOperationException(
                        "cannot cast type character / character varying / text to boolean");
            default:
                throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
            }
        }

    }

    public static class BooleanConstant extends OushuDBConstant {

        private final boolean val;

        public BooleanConstant(boolean val) {
            this.val = val;
        }

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.BOOLEAN;
        }

        @Override
        public boolean getBoolean() {
            return val;
        }

        @Override
        public Object getValue() {
            return val;
        }

        @Override
        public String getTextRepresentation() {
            return val ? "TRUE" : "FALSE";
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return val ? "TRUE" : "FALSE";
        }

        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            switch (dataType) {
            case SMALLINT:
                return new SmallintConstant((short) (val ? 1 : 0));
            case INTEGER:
                return new IntegerConstant(val ? 1 : 0);
            case BIGINT:
                return new BigintConstant(val ? 1L : 0L);
            case CHAR:
            case VARCHAR:
            case TEXT:
                throw new IllegalArgumentException("cannot cast type boolean to text");
            case BOOLEAN:
                return this;
            default:
                throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
            }
        }

    }

    public static class NullConstant extends OushuDBConstant {

        @Override
        public OushuDBDataType getDataType() {
            return OushuDBDataType.NULL;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public String getUnquotedStringRepresentation() {
            return "NULL";
        }

        /**
         * Casting null to any data type results in a null constant.
         */
        @Override
        public OushuDBConstant cast(OushuDBDataType dataType) {
            return this;
        }

        @Override
        public int hashCode() {
            return 0;
        }

    }

    private static final NullConstant nullConstant = new NullConstant();
    private static final BooleanConstant trueConstant = new BooleanConstant(true);
    private static final BooleanConstant falseConstant = new BooleanConstant(false);

    public static OushuDBConstant createConstant(OushuDBDataType dataType, Object value) {
        switch (dataType) {
        case SMALLINT:
            return new SmallintConstant((short) value);
        case INTEGER:
            return new IntegerConstant((int) value);
        case BIGINT:
            return new BigintConstant((long) value);
        case NUMERIC:
            return new NumericConstant((BigDecimal) value);
        case CHAR:
        case VARCHAR:
        case TEXT:
            return new StringConstant((String) value);
        case BOOLEAN:
            return new BooleanConstant((boolean) value);
        default:
            throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
        }
    }

    public static OushuDBConstant createNullConstant() {
        return nullConstant;
    }

    @Override
    public OushuDBDataType getExpressionType() {
        return getDataType();
    }

    /* get value as Java data types */

    @Override
    public OushuDBConstant getExpectedValue() {
        return this;
    }

    public abstract OushuDBDataType getDataType();

    public short getShort() {
        throw new UnsupportedOperationException("getShort() is not supported for current sub-class");
    }

    public int getInt() {
        throw new UnsupportedOperationException("getInt() is not supported for current sub-class");
    }

    public long getLong() {
        throw new UnsupportedOperationException("getLong() is not supported for current sub-class");
    }

    public BigDecimal getBigDecimal() {
        throw new UnsupportedOperationException("getBigDecimal() is not supported for current sub-class");
    }

    public String getString() {
        throw new UnsupportedOperationException("getString() is not supported for current sub-class");
    }

    public boolean getBoolean() {
        throw new UnsupportedOperationException("getBoolean() is not supported for current sub-class");
    }

    /**
     * @return internal Java value
     */
    public abstract Object getValue();

    /**
     * @return text representation in SQL statements
     */
    public abstract String getTextRepresentation();

    /**
     * @return text representation in Java format
     */
    public abstract String getUnquotedStringRepresentation();

    /**
     * @param dataType
     *            target data SQL data type
     *
     * @return constant of {@code dataType} target type
     */
    public abstract OushuDBConstant cast(OushuDBDataType dataType);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    @Override
    public boolean equals(Object object) {
        return (object instanceof OushuDBConstant)
                && (this == object || ((OushuDBConstant) object).getValue().equals(this.getValue()));
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

}
