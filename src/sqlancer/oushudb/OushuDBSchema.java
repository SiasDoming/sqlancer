package sqlancer.oushudb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.oushudb.OushuDBSchema.OushuDBTable;
import sqlancer.oushudb.ast.OushuDBConstant;

public class OushuDBSchema extends AbstractSchema<OushuDBGlobalState, OushuDBTable> {

    public static enum OushuDBDataType {
        SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE_PRECISION, SMALLSERIAL, SERIAL, BIGSERIAL, CHAR, VARCHAR, TEXT,
        BYTEA, TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ, DATE, INTERVAL, BOOLEAN, POINT, LSEG, BOX, PATH, POLYGON, CIRCLE,
        CIDR, INET, MACADDR, BIT, VARBIT, ARRAY,
        NULL; // XXX: NULL is not a real type but in most implements it's better to distinct NULL from others

        // private final String[] sqlTextRepresentations;

        // TODO: support CHAR (without VAR)
        protected static List<OushuDBDataType> knownDataTypesByPQS = Arrays.asList(SMALLINT, INTEGER, BIGINT, VARCHAR, TEXT, BOOLEAN);

        public static OushuDBDataType fromTypeString(String typeString) {
            switch (typeString) {
            case "smallint":
                return SMALLINT;
            case "integer":
                return INTEGER;
            case "bigint":
                return BIGINT;
            case "decimal":
            case "numeric":
                return NUMERIC;
            case "real":
                return REAL;
            case "float":
            case "double precision":
                return DOUBLE_PRECISION;
            case "character":
                return CHAR;
            case "character varying":
                return VARCHAR;
            case "text":
                return TEXT;
            case "boolean":
                return BOOLEAN;
            case "bit":
                return BIT;
            case "bit varying":
                return VARBIT;
            case "inet":
                return INET;
            default:
                throw new IllegalArgumentException("unknown data type name: " + typeString);
            }
        }

        public static List<OushuDBDataType> getKnownDataTypesByPQS() {
            return knownDataTypesByPQS;
        }

        // public List<String> getSQLTextRepresentations() {
        // return sqlTextRepresentations;
        // }

    }

    public static class OushuDBColumn extends AbstractTableColumn<OushuDBTable, OushuDBDataType> {

        public static OushuDBColumn createDummy(String columnName) {
            return new OushuDBColumn(columnName, OushuDBDataType.INTEGER);
        }

        public OushuDBColumn(String columnName, OushuDBDataType columnType) {
            super(columnName, null, columnType);
        }

    }

    public static class OushuDBIndex extends TableIndex {

        public OushuDBIndex(String indexName) {
            super(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().equals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static class OushuDBTable extends AbstractRelationalTable<OushuDBColumn, OushuDBIndex, OushuDBGlobalState> {

        public static enum TableType {
            BASE_TABLE, TEMPORARY, VIEW;

            public static TableType fromTableKind(String str) {
                if (str.equals("BASE TABLE")) {
                    return BASE_TABLE;
                } else if (str.equals("VIEW")) {
                    return VIEW;
                } else if (str.equals("LOCAL TEMPORARY")) {
                    return TEMPORARY;
                } else {
                    throw new AssertionError("unknown table type: " + str);
                }
            }

        }

        public static enum TableStorage {
            HEAP, APPEND_ONLY, ORC, PARQUET, MAGMAAP, MAGMATP, EXTERNAL, FOREIGN, HORC, HPARQUET, NONE;

            public static TableStorage fromRelStorage(String relStorage) {
                switch (relStorage) {
                case "h":
                    return HEAP;
                case "x":
                    return EXTERNAL;
                case "a":
                    return APPEND_ONLY;
                case "v":
                    return NONE;
                case "o":
                    return ORC;
                case "m":
                    return MAGMAAP;
                case "t":
                    return MAGMATP;
                case "p":
                    return PARQUET;
                case "f":
                    return FOREIGN;
                case "q":
                    return HPARQUET;
                case "r":
                    return HORC;
                default:
                    throw new IllegalArgumentException("unknown relation storage type: " + relStorage);
                }
            }

            public List<OushuDBDataType> getSupportedDataTypes() {
                List<OushuDBDataType> supported = Arrays.asList(OushuDBDataType.values());
                switch (this) {
                case ORC:
                case MAGMAAP:
                case MAGMATP:
                case HORC:
                case HPARQUET:
                    supported.remove(OushuDBDataType.BIT);
                    supported.remove(OushuDBDataType.VARBIT);
                    supported.remove(OushuDBDataType.ARRAY);
                    break;
                case NONE:
                    return Collections.emptyList();
                default:
                    break;
                }
                return supported;
            }

            public boolean isDataTypeSupported(OushuDBDataType dataType) {
                switch (this) {
                case ORC:
                case MAGMAAP:
                case MAGMATP:
                case HORC:
                case HPARQUET:
                    return dataType != OushuDBDataType.BIT && dataType != OushuDBDataType.VARBIT
                            && dataType != OushuDBDataType.ARRAY;
                case NONE:
                    return false;
                default:
                    return true;
                }
            }

        }

        /**
         * table type
         */
        private final TableType tableType;
        /**
         * storage type
         */
        private final TableStorage tableStorage;
        /**
         * whether the table is insertable or readonly
         */
        private final boolean insertable;

        public OushuDBTable(String name, List<OushuDBColumn> columns, List<OushuDBIndex> indexes, TableType tableType,
                TableStorage tableStorage, boolean insertable) {
            super(name, columns, indexes, tableType == TableType.VIEW);
            this.tableType = tableType;
            this.tableStorage = tableStorage;
            this.insertable = insertable;
        }

        public TableType getTableType() {
            return tableType;
        }

        public TableStorage getTableStorage() {
            return tableStorage;
        }

        public boolean isInsertable() {
            return insertable;
        }

    }

    public static class OushuDBTables extends AbstractTables<OushuDBTable, OushuDBColumn> {

        public OushuDBTables(List<OushuDBTable> tables) {
            super(tables);
        }

        public OushuDBRowValue getRandomRowValue(SQLConnection connection) throws SQLException {
            String sqlString = String.format(
                "SELECT %s FROM %s ORDER BY random() LIMIT 1",
                columnNamesAsString(c -> String.format("%1$s AS \"%1$s\"", c.getFullQualifiedName())),
                tableNamesAsString());
            Map<OushuDBColumn, OushuDBConstant> values = new HashMap<>();
            try (Statement statement = connection.createStatement()) {
                ResultSet randomRow = statement.executeQuery(sqlString);
                if (!randomRow.next()) {
                    throw new AssertionError("could not find random row! ");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    OushuDBColumn column = getColumns().get(i);
                    OushuDBConstant constant;
                    if (randomRow.getObject(i) == null) {
                        constant = OushuDBConstant.createNullConstant();
                    } else {
                        constant = OushuDBConstant.createConstant(column.getType(), randomRow.getObject(i));
                    }
                    values.put(column, constant);
                }
                assert !randomRow.next();
                return new OushuDBRowValue(this, values);
            }
        }
    }

    public static class OushuDBRowValue extends AbstractRowValue<OushuDBTables, OushuDBColumn, OushuDBConstant> {

        public OushuDBRowValue(OushuDBTables tables, Map<OushuDBColumn, OushuDBConstant> values) {
            super(tables, values);
        }

    }

    public static class OushuDBTablespace {
        private final String tablespaceName;
        private final OushuDBFilespaceType filespaceType;

        public OushuDBTablespace(String tablespaceName, OushuDBFilespaceType filespaceType) {
            this.tablespaceName = tablespaceName;
            this.filespaceType = filespaceType;
        }

        public String getTablespaceName() {
            return tablespaceName;
        }

        public OushuDBFilespaceType getFilespaceType() {
            return filespaceType;
        }

    }

    public static enum OushuDBFilespaceType {
        PG(OushuDBTable.TableStorage.HEAP),
        HDFS(OushuDBTable.TableStorage.APPEND_ONLY, OushuDBTable.TableStorage.ORC, OushuDBTable.TableStorage.EXTERNAL,
                OushuDBTable.TableStorage.HORC),
        S3(OushuDBTable.TableStorage.APPEND_ONLY, OushuDBTable.TableStorage.ORC, OushuDBTable.TableStorage.EXTERNAL,
                OushuDBTable.TableStorage.HORC),
        MAGMA(OushuDBTable.TableStorage.MAGMAAP, OushuDBTable.TableStorage.MAGMATP);

        private final List<OushuDBTable.TableStorage> supportedStorageTypes;

        private OushuDBFilespaceType(OushuDBTable.TableStorage... storageTypes) {
            this.supportedStorageTypes = Arrays.asList(storageTypes);
        }

        public List<OushuDBTable.TableStorage> getSupportedStorageTypes() {
            return supportedStorageTypes;
        }

    }

    public static OushuDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try (Statement s = con.createStatement()) {
            List<OushuDBTable> databaseTables = fetchDatabaseTables(s);
            List<OushuDBTablespace> availableTablespaces = fetchTablespaces(s);
            return new OushuDBSchema(databaseTables, databaseName, availableTablespaces);
        }
    }

    /**
     * fetch all table information in the connected database
     *
     * @param statement
     *            JDBC SQL statement connected to database
     *
     * @return list of tables
     *
     * @throws SQLException
     */
    private static List<OushuDBTable> fetchDatabaseTables(Statement statement) throws SQLException {
        List<OushuDBTable> databaseTables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(
                // similiar to the system view information_schema.tables but add storage column and exclude child tables
                "SELECT c.relname AS table_name, c.relstorage AS table_storage, "
                        + "CASE WHEN c.relnamespace = pg_my_temp_schema() THEN 'LOCAL TEMPORARY' "
                        + "WHEN c.relkind = 'r' THEN 'BASE TABLE' " + "WHEN c.relkind = 'v' THEN 'VIEW' "
                        + "ELSE null END AS table_type, "
                        + "CASE WHEN (c.relkind = 'v' OR c.relstorage = 'f' OR (c.relstorage = 'x' AND x.writable = 'f')) "
                        + "THEN 'NO' ELSE 'YES' END AS is_insertable_into "
                        + "FROM pg_class_internal c LEFT JOIN pg_exttable x ON c.oid = x.reloid "
                        + "WHERE c.relkind IN ('r', 'v') " + "AND c.relnamespace IN (2200, pg_my_temp_schema()) "
                        + "AND c.oid NOT IN (SELECT inhrelid FROM pg_catalog.pg_inherits) ORDER BY table_name")) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                OushuDBTable.TableStorage tableStorage = OushuDBTable.TableStorage
                        .fromRelStorage(rs.getString("table_storage"));
                OushuDBTable.TableType tableType = OushuDBTable.TableType
                        .fromTableKind(rs.getString("table_type"));
                boolean insertable = rs.getBoolean("is_insertable_into");
                // fetch column information
                List<OushuDBColumn> tableColumns = fetchTableColumns(statement, tableName);
                // fetch index information
                List<OushuDBIndex> tableIndexes = fetchTableIndexes(statement, tableName);
                OushuDBTable table = new OushuDBTable(tableName, tableColumns, tableIndexes, tableType, tableStorage,
                        insertable);

                for (OushuDBColumn c : tableColumns) {
                    c.setTable(table);
                }
                databaseTables.add(table);
            }
        }
        return databaseTables;
    }

    /**
     * fetch column information of the given table {@code tableName}
     *
     * @param statement
     *            JDBC SQL statement connected to database
     * @param tableName
     *            name of the table
     *
     * @return list of columns
     *
     * @throws SQLException
     */
    private static List<OushuDBColumn> fetchTableColumns(Statement statement, String tableName) throws SQLException {
        List<OushuDBColumn> tableColumns = new ArrayList<>();
        try (ResultSet crs = statement
                .executeQuery("select column_name, data_type from information_schema.columns where table_name = '"
                        + tableName + "' ORDER BY column_name")) {
            /**
             *
             * SELECT c.relname AS table_name, c.relstorage AS table_storage, CASE WHEN c.relnamespace = pg_my_temp_schema()
             * THEN 'LOCAL TEMPORARY' WHEN c.relkind = 'r' THEN 'BASE TABLE' WHEN c.relkind = 'v' THEN 'VIEW' ELSE null END AS
             * table_type, CASE WHEN (c.relkind = 'v' OR c.relstorage = 'f' OR (c.relstorage = 'x' AND x.writable = 'f')) THEN
             * 'NO' ELSE 'YES' END AS is_insertable_into FROM pg_class_internal c LEFT JOIN pg_exttable x ON c.oid = x.reloid
             * WHERE c.relkind IN ('r', 'v') AND c.relnamespace IN (2200, pg_my_temp_schema()) AND c.oid NOT IN (SELECT inhrelid
             * FROM pg_catalog.pg_inherits) ORDER BY table_name;
             *
             */
            while (crs.next()) {
                String columnName = crs.getString("column_name");
                OushuDBDataType dataType = OushuDBDataType.fromTypeString(crs.getString("data_type"));
                OushuDBColumn column = new OushuDBColumn(columnName, dataType);
                tableColumns.add(column);
            }
        }
        return tableColumns;
    }

    /**
     * fetch index information of the given table {@code tableName}
     *
     * @param statement
     *            JDBC SQL statement connected to database
     * @param tableName
     *            name of the table
     *
     * @return list of indexes
     *
     * @throws SQLException
     */
    private static List<OushuDBIndex> fetchTableIndexes(Statement statement, String tableName) throws SQLException {
        List<OushuDBIndex> tableIndexes = new ArrayList<>();
        try (ResultSet irs = statement.executeQuery(
                "SELECT indexname FROM pg_indexes WHERE tablename='" + tableName + "' ORDER BY indexname;")) {
            while (irs.next()) {
                String indexName = irs.getString("indexname");
                if (DBMSCommon.matchesIndexName(indexName)) {
                    tableIndexes.add(new OushuDBIndex(indexName));
                }
            }
        }
        return tableIndexes;
    }

    private static List<OushuDBTablespace> fetchTablespaces(Statement s) {
        // FIXME: fetch information from catalog instead of using hard code
        List<OushuDBTablespace> tablespaces = new ArrayList<>();
        tablespaces.add(new OushuDBTablespace("magma_default", OushuDBFilespaceType.MAGMA));
        tablespaces.add(new OushuDBTablespace("dfs_default", OushuDBFilespaceType.HDFS));
        return tablespaces;
    }

    private final String databaseName;

    private final List<OushuDBTablespace> availableTablespaces;

    public OushuDBSchema(List<OushuDBTable> databaseTables, String databaseName, List<OushuDBTablespace> tablespaces) {
        super(databaseTables);
        this.databaseName = databaseName;
        this.availableTablespaces = tablespaces;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<OushuDBTablespace> getAvailableTablespaces() {
        return availableTablespaces;
    }

    public OushuDBTables getRandomNonEmptyTables() {
        return new OushuDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
