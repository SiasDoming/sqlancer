package sqlancer.oushudb.ast;

import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;

public class OushuDBColumnValue implements OushuDBExpression {
    
    /**
     * referenced column
     */
    private final OushuDBColumn column;
    /**
     * expected actual value of this column in the pivot row
     */
    private final OushuDBConstant expectedValue;
    
    public OushuDBColumnValue(OushuDBColumn column, OushuDBConstant expectedValue) {
        this.column = column;
        this.expectedValue = expectedValue;
    }

    @Override
    public OushuDBDataType getExpressionType() {
        return column.getType();
    }

    @Override
    public OushuDBConstant getExpectedValue() {
        return expectedValue;
    }

    public OushuDBColumn getColumn() {
        return column;
    }

}
