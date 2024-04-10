package sqlancer.oushudb;

import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.oushudb.ast.OushuDBConstant;
import sqlancer.oushudb.ast.OushuDBExpression;

public class OushuDBToStringVisitor extends ToStringVisitor<OushuDBExpression> {

    @Override
    public void visitSpecific(OushuDBExpression expr) {
        if (expr instanceof OushuDBConstant) {
            visit((OushuDBConstant) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    public void visit(OushuDBConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

}