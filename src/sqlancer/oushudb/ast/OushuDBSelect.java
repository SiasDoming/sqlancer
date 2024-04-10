package sqlancer.oushudb.ast;

import java.util.List;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.oushudb.OushuDBVisitor;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBTable;

public class OushuDBSelect extends SelectBase<OushuDBExpression>
    implements OushuDBExpression, Select<OushuDBJoin, OushuDBExpression, OushuDBTable, OushuDBColumn> {
    
    public static class OushuDBFromTable implements OushuDBExpression {

        /**
         * source table
         */
        private final OushuDBTable table;
        /**
         * ONLY specifies whether to scan all descendant tables of the source table
         */
        private final boolean only;

        public OushuDBFromTable(OushuDBTable table, boolean only) {
            this.table = table;
            this.only = only;
        }

        public OushuDBTable getTable() {
            return table;
        }

        public boolean isOnly() {
            return only;
        }

    }

    // public static enum SelectType {
    //     ALL, DISTINCT;
    // }

    private List<OushuDBJoin> joinClauses;
    // TODO: support DISTINCT clause
    // private SelectType selectType = SelectType.ALL;
    // private OushuDBExpression distinctOnClause;
    // TODO: support FOR clause

    @Override
    public String asString() {
        return OushuDBVisitor.asString(this);
    }

    @Override
    public List<OushuDBJoin> getJoinClauses() {
        return joinClauses;
    }

    @Override
    public void setJoinClauses(List<OushuDBJoin> joinStatements) {
        this.joinClauses = joinStatements;
    }

    
}