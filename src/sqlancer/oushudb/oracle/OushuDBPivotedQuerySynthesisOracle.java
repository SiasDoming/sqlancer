package sqlancer.oushudb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import com.mysql.cj.xdevapi.Expression;

import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.oushudb.OushuDBGlobalState;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;
import sqlancer.oushudb.OushuDBSchema.OushuDBRowValue;
import sqlancer.oushudb.OushuDBSchema.OushuDBTables;
import sqlancer.oushudb.ast.OushuDBColumnValue;
import sqlancer.oushudb.ast.OushuDBConstant;
import sqlancer.oushudb.ast.OushuDBExpression;
import sqlancer.oushudb.ast.OushuDBSelect;
import sqlancer.oushudb.ast.OushuDBSelect.OushuDBFromTable;
import sqlancer.oushudb.gen.OushuDBExpressionGenerator;

public class OushuDBPivotedQuerySynthesisOracle extends PivotedQuerySynthesisBase<OushuDBGlobalState, OushuDBRowValue, OushuDBExpression, SQLConnection> {

    public OushuDBPivotedQuerySynthesisOracle(OushuDBGlobalState globalState) throws SQLException {
        super(globalState);
    }

    @Override
    protected Query<SQLConnection> getRectifiedQuery() throws Exception {
        OushuDBTables randomFromTables = globalState.getSchema().getRandomNonEmptyTables();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        OushuDBSelect selectStatement = new OushuDBSelect();
        selectStatement.setFromList(randomFromTables.getTables().stream()
            .map(t -> new OushuDBFromTable(t, false)).collect(Collectors.toList()));
        selectStatement.setFetchColumns(randomFromTables.getColumns().stream()
            .map(c -> new OushuDBColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c))).collect(Collectors.toList()));
        selectStatement.setWhereClause(generateRectifiedExpression(randomFromTables));
    }

    /**
     * Prevent name collisions by aliasing the column as table_column.
     * @param column original column
     * @return aliased column
     */
    private OushuDBColumn getFetchValueAliasedColumn(OushuDBColumn column) {
        String aliasName = String.format("%s AS %s_%s", column.getFullQualifiedName(), column.getTable().getName(), column.getName());
        OushuDBColumn aliasColumn = new OushuDBColumn(aliasName, column.getType());
        aliasColumn.setTable(column.getTable());
        return aliasColumn;
    }

    /**
     * Generate predicates that the pivot row always meets
     * @param rowValue pivot row
     * @return predicate expression
     */
    private OushuDBExpression generateRectifiedExpression(OushuDBRowValue rowValue) {
        // generate boolean expresion on given pivot row
        OushuDBExpressionGenerator generator = new OushuDBExpressionGenerator(globalState);
        generator.setColumns(new ArrayList<>(rowValue.getValues().keySet()));
        generator.setRowValue(rowValue);
        OushuDBExpression expression = generator.generateExpressionWithExpectedResult(OushuDBDataType.BOOLEAN);

        // build predicate by expected value
        OushuDBExpression predicate;
        if (expression.getExpectedValue().isNull()) {
            predicate = OushuDBPostfixOperation.create(expression, PostfixOperator.IS_NULL);
        } else {
            predicate = OushuDBPostfixOperation.create(expression,
                expression.getExpectedValue().getBoolean() ? PostfixOperator.IS_TRUE : PostfixOperator.IS_FALSE);
        }
        rectifiedPredicates.add(predicate);
        return predicate;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> pivotRowQuery) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getExpectedValues(OushuDBExpression expr) {
        // TODO: support more expressions
        if (expr instanceof OushuDBConstant) {
            return ((OushuDBConstant) expr).getTextRepresentation();
        } else {
            throw new AssertionError(expr);
        }
    }

}
