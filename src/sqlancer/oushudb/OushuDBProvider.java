package sqlancer.oushudb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.oushudb.OushuDBOptions.OushuDBOracleFactory;
import sqlancer.oushudb.gen.OushuDBInsertGenerator;
import sqlancer.oushudb.gen.OushuDBTableGenerator;

@AutoService(DatabaseProvider.class)
public class OushuDBProvider extends SQLProviderAdapter<OushuDBGlobalState, OushuDBOptions> {

    protected enum Action implements AbstractAction<OushuDBGlobalState> {
        // ALTER_TABLE,
        // ANALYZE,
        // COMMENT_ON,
        // CREATE_SEQUENCE,
        // DELETE,
        // DROP_SEQUENCE,
        INSERT(OushuDBInsertGenerator::insert);
        // RESET,
        // SET,
        // TRUNCATE,
        // UPDATE;

        private final SQLQueryProvider<OushuDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<OushuDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(OushuDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }

    }

    /**
     * generate only data types and expressions that are understood by PQS.
     */
    public static boolean generateOnlyKnown;

    /**
     * map actions to the corresponding number of execution times
     * @param globalState global state
     * @param action action type
     * @return number of times the action to be executed
     */
    protected static int mapActions(OushuDBGlobalState globalState, Action action) {
        Randomly randomly = globalState.getRandomly();
        switch (action) {
        case INSERT:
            return randomly.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        default:
            throw new AssertionError(action);
        }
    }

    /**
     * create a number of tables in the connected database
     * @param globalState global state
     * @param numTables number of tables to be generated
     * @throws Exception if a database access error or a logging error occurs
     */
    protected static void createTables(OushuDBGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = OushuDBTableGenerator.generate(tableName, globalState.getSchema(),
                        globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    /**
     * prepare tables by apply a number of DML statements
     * @param globalState global state
     * @throws Exception if a database access error or a logging error occurs
     */
    protected static void prepareTables(OushuDBGlobalState globalState) throws Exception {
        StatementExecutor<OushuDBGlobalState, Action> executor = new StatementExecutor<OushuDBGlobalState, OushuDBProvider.Action>(
                globalState, Action.values(), OushuDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        executor.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 5000"));
    }

    public OushuDBProvider() {
        super(OushuDBGlobalState.class, OushuDBOptions.class);
    }

    @Override
    public void generateDatabase(OushuDBGlobalState globalState) throws Exception {
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);
    }

    @Override
    public SQLConnection createDatabase(OushuDBGlobalState globalState) throws Exception {
        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == OushuDBOracleFactory.PQS)) {
            generateOnlyKnown = true;
        }

        String userName = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String entryURL = globalState.getDbmsSpecificOptions().connectionURL;
        // trim URL to exclude "jdbc:"
        if (entryURL.startsWith("jdbc:")) {
            entryURL = entryURL.substring(5);
        }
        // manually parse JDBC URL into format prefix(postgresql://<host>[:port]/) + entryDatabaseName (+
        // suffix(?key=value[&...]))
        String prefix = entryURL.substring(0, entryURL.indexOf('/', 14) + 1);
        String suffix = entryURL.indexOf('?') < 0 ? "" : entryURL.substring(entryURL.indexOf('?'));
        String entryDatabaseName = entryURL.substring(prefix.length(), entryURL.length() - suffix.length());
        String databaseName = globalState.getDatabaseName();

        // connect to entry database and truncate target database for this thread
        Connection entryConnection = DriverManager.getConnection("jdbc:" + entryURL, userName, password);
        globalState.getState().logStatement("\\c " + entryDatabaseName);
        String dropDatabaseCommand = "DROP DATABASE IF EXISTS " + databaseName;
        String createDatabaseCommand = "CREATE DATABASE " + databaseName + "WITH ENCODING 'UTF8' TEMPLATE template0";
        globalState.getState().logStatement(dropDatabaseCommand);
        globalState.getState().logStatement(createDatabaseCommand);
        try (Statement s = entryConnection.createStatement()) {
            s.execute(dropDatabaseCommand);
            s.execute(createDatabaseCommand);
        }
        entryConnection.close();

        // connect to test database
        int databaseIndex = entryURL.indexOf(entryDatabaseName);
        String preDatabaseName = entryURL.substring(0, databaseIndex);
        String postDatabaseName = entryURL.substring(databaseIndex + entryDatabaseName.length());
        String testURL = preDatabaseName + databaseName + postDatabaseName;
        Connection testConnection = DriverManager.getConnection("jdbc:" + testURL, userName, password);
        globalState.getState().logStatement("\\c " + databaseName);

        return new SQLConnection(testConnection);
    }

    @Override
    public String getDBMSName() {
        return "OushuDB";
    }

}
