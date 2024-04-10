package sqlancer.oushudb.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;
import sqlancer.oushudb.OushuDBSchema.OushuDBTable;

public class OushuDBJoin implements OushuDBExpression, Join<OushuDBExpression, OushuDBTable, OushuDBColumn> {

    private OushuDBExpression onClause;

    @Override
    public OushuDBConstant getExpectedValue() {
        throw new AssertionError();
    }

    @Override
    public OushuDBDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public Expression<OushuDBColumn> getOnClause() {
        return onClause;
    }

    @Override
    public void setOnClause(OushuDBExpression onClause) {
        this.onClause = onClause;
    }
}
