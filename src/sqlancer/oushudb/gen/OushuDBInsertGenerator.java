package sqlancer.oushudb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oushudb.OushuDBGlobalState;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBTable;
import sqlancer.oushudb.ast.OushuDBConstant;

/**
 * Gernerator for {@code INSERT} statements to create new rows in a table
 */
public class OushuDBInsertGenerator {

    public static SQLQueryAdapter insert(OushuDBGlobalState globalState) {
        OushuDBTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        StringBuilder sqlBuilder = new StringBuilder();
        ExpectedErrors errors = new ExpectedErrors();

        // specify columns to be explicitly assigned
        sqlBuilder.append("INSERT INTO ").append(table.getName());
        List<OushuDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sqlBuilder.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ", "(", ")")));

        if (Randomly.getBooleanWithSmallProbability()) {
            sqlBuilder.append(" DEFAULT VALUES");
        } else {
            sqlBuilder.append(" VALUES");
            int n = Randomly.smallNumber() + 1;
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sqlBuilder.append(", ");
                }
                appendRowValue(globalState, sqlBuilder, columns);
            }
        }
        // TODO: support insertion by query

        return new SQLQueryAdapter(sqlBuilder.toString(), errors);
    }

    private static void appendRowValue(OushuDBGlobalState globalState, StringBuilder sqlBuilder,
            List<OushuDBColumn> columns) {
        sqlBuilder.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sqlBuilder.append(", ");
            }
            if (Randomly.getBooleanWithSmallProbability()) {
                sqlBuilder.append("DEFAULT");
            } else {
                OushuDBConstant constant = OushuDBExpressionGenerator.generateConstant(globalState,
                        columns.get(i).getType());
                sqlBuilder.append(constant.getTextRepresentation());
                // TODO: generate complex expressions
                // } else {
                // sqlBuilder.append(new OushuDBToStringVisitor().visit(OushuDBExpressionGenerator.););
            }
        }
    }
}