package sqlancer.oushudb;

import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.oushudb.ast.OushuDBConstant;
import sqlancer.oushudb.ast.OushuDBExpression;

public interface OushuDBVisitor extends ToStringVisitor<OushuDBExpression> {

    static String asString(OushuDBExpression expression) {
        OushuDBToStringVisitor visitor = new OushuDBToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

    static String asExpectedValue(OushuDBExpression expression) {
        OushuDBExpectedValueVisitor visitor = new OushuDBExpectedValueVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

    void visit(OushuDBConstant constant);

    String get();

    default void visit(OushuDBExpression expression) {
        if (expression instanceof OushuDBConstant) {
            visit((OushuDBConstant) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

}
