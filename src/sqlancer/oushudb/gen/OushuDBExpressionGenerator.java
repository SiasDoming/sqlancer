package sqlancer.oushudb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.oushudb.OushuDBGlobalState;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;
import sqlancer.oushudb.OushuDBSchema.OushuDBRowValue;
import sqlancer.oushudb.ast.OushuDBColumnValue;
import sqlancer.oushudb.ast.OushuDBConstant;
import sqlancer.oushudb.ast.OushuDBExpression;


public class OushuDBExpressionGenerator {

    /**
     * global state
     */
    private final OushuDBGlobalState globalState;
    /**
     * available columns
     */
    private List<OushuDBColumn> availableColumns;
    /**
     * pivot row
     */
    private OushuDBRowValue rowValue;

    public OushuDBExpressionGenerator(OushuDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public OushuDBExpression generateExpressionWithExpectedResult(OushuDBDataType dataType) {
        OushuDBExpression expression;
        do {
            expression = generateExpression(0, dataType);
        } while (expression.getExpectedValue() == null);
        return expression;
    }

    private OushuDBExpression generateExpression(int depth, OushuDBDataType dataType) {
        // TODO: generate expressions of compatible data types that could be implicly casted
        // dataType = getImplicitlyCompatibleType(dataType);
        // generic simple constant or column expression
        if (depth > globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            if (Randomly.getBooleanWithRatherLowProbability() || filterColumns(dataType).isEmpty()) {
                return generateConstant(globalState, dataType);
            } else {
                return generateColumnValue(dataType);
            }
        } else {
            switch (dataType) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case NUMERIC:
            case REAL:
            case DOUBLE_PRECISION:
                return generateSmallIntExpression(depth);
            case CHAR:
            case VARCHAR:
            case TEXT:
                // TODO: character type
            case BOOLEAN:
            default:
                break;
            }
        }
    }

    private static enum ExpressionOperator {
        PREFIX_OPERATOR, POSTFIX_OPERATOR, BINARY_OPERATOR, TERNARY_OPERATOR,
        CAST, FUNCTION, BETWEEN, IN
    }

    private OushuDBColumnValue generateColumnValue(OushuDBDataType dataType) {
        OushuDBColumn column = Randomly.fromList(filterColumns(dataType));
        OushuDBConstant value = rowValue == null ? null : rowValue.getValues().get(column);
        return new OushuDBColumnValue(column, value);
    }

    /**
     * Generate a constant expression of the given data type
     * @param globalState global state
     * @param dataType SQL data type
     * @return constant expression
     */
    public static OushuDBConstant generateConstant(OushuDBGlobalState globalState, OushuDBDataType dataType) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return OushuDBConstant.createNullConstant();
        }
        Randomly randomly = globalState.getRandomly();
        switch (dataType) {
        // SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE_PRECISION, SMALLSERIAL, SERIAL, BIGSERIAL,
        // CHAR, VARCHAR, TEXT, BYTEA, TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ, DATE, INTERVAL,
        // BOOLEAN, POINT, LSEG, BOX, PATH, POLYGON, CIRCLE, CIDR, INET, MACADDR, BIT, VARBIT,
        // ARRAY
        case SMALLINT:
            return OushuDBConstant.createConstant(
                    Randomly.getBooleanWithRatherLowProbability() ? OushuDBDataType.TEXT : dataType,
                    (short) randomly.getInteger(Short.MIN_VALUE, Short.MAX_VALUE));
        case INTEGER:
            return OushuDBConstant.createConstant(
                    Randomly.getBooleanWithRatherLowProbability() ? OushuDBDataType.TEXT : dataType,
                    (int) randomly.getInteger(Integer.MIN_VALUE, Integer.MAX_VALUE));
        case BIGINT:
            return OushuDBConstant.createConstant(
                    Randomly.getBooleanWithRatherLowProbability() ? OushuDBDataType.TEXT : dataType,
                    randomly.getLong(Long.MIN_VALUE, Long.MAX_VALUE));
        case NUMERIC:
            return OushuDBConstant.createConstant(dataType, randomly.getRandomBigDecimal());
        case REAL:
            return OushuDBConstant.createConstant(dataType, (float) randomly.getDouble());
        case DOUBLE_PRECISION:
            return OushuDBConstant.createConstant(dataType, randomly.getDouble());
        default:
            throw new IllegalArgumentException(String.format("SQL data type %s is not supported yet", dataType));
        }
    }



    public void setAvailableColumns(List<OushuDBColumn> availableColumns) {
        this.availableColumns = availableColumns;
    }
    
}
