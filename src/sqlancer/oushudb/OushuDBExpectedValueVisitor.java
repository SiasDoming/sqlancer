package sqlancer.oushudb;

import java.util.List;

import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.oushudb.ast.OushuDBConstant;
import sqlancer.oushudb.ast.OushuDBExpression;

public class OushuDBExpectedValueVisitor extends ToStringVisitor<OushuDBExpression> {

    /** 
     * number of tabs '\t' to be added for pretty-printing as tree structures
     */
    private static int prefixTabs = 0;

    @Override
    public void visit(BinaryOperation<OushuDBExpression> op) {
        // FIXME: visit op and sub-expr in right order
        // print the expected value of this expression
        for (int i = 0; i < prefixTabs; i++) {
            sb.append('\t');
        }
        OushuDBToStringVisitor v = new OushuDBToStringVisitor();
        v.visit(op);
        sb.append(v.get());
    }

    @Override
    public void visit(UnaryOperation<OushuDBExpression> op) {
        // FIXME: visit op and sub-expr in right order
        super.visit(op);
    }

    @Override
    public void visit(List<OushuDBExpression> expressions) {
        sb.append("[\n");
        prefixTabs++;
        super.visit(expressions);
        sb.append("]\n");
        prefixTabs--;
    }

    @Override
    public void visitSpecific(OushuDBExpression expr) {
        if (expr instanceof OushuDBConstant) {
            visit((OushuDBConstant) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    public void visit(OushuDBConstant constant) {
        for (int i = 0; i < prefixTabs; i++) {
            sb.append('\t');
        }
        super.visit(constant);
        // no need to double-printing the value for constant expression
        // sb.append(" -- ");
        // sb.append(constant.getTextRepresentation());
        sb.append('\n');
    }

}