package sqlancer.oushudb.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;

public interface OushuDBExpression extends Expression<OushuDBColumn> {

    /**
     * @return expected data type of the result
     */
    default OushuDBDataType getExpressionType() {
        return null;
    }

    /**
     * evaluate the expression and return the result value
     * @return expected value
     */
    default OushuDBConstant getExpectedValue() {
        return null;
    }

}