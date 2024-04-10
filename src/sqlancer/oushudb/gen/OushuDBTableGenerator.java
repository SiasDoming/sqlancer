package sqlancer.oushudb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oushudb.OushuDBGlobalState;
import sqlancer.oushudb.OushuDBProvider;
import sqlancer.oushudb.OushuDBSchema;
import sqlancer.oushudb.OushuDBSchema.OushuDBColumn;
import sqlancer.oushudb.OushuDBSchema.OushuDBDataType;
import sqlancer.oushudb.OushuDBSchema.OushuDBTable;
import sqlancer.oushudb.OushuDBSchema.OushuDBTablespace;

public class OushuDBTableGenerator {

    /**
     * name of the generated table
     */
    private final String tableName;
    /**
     * schema the table belongs to
     */
    private final OushuDBSchema schema;
    /**
     * global state
     */
    private final OushuDBGlobalState globalState;
    /**
     * whether the primary key is already specified
     */
    @SuppressWarnings("unused")
    private boolean hasPrimaryKey;
    /**
     * SQL statement builder
     */
    private StringBuilder sqlBuilder;
    /**
     * generated columns
     */
    private List<OushuDBColumn> columns;
    /**
     * generated table type
     */
    private OushuDBTable.TableType tableType;
    /**
     * generated table storage format
     */
    private OushuDBTable.TableStorage tableStorage;
    /**
     * expected error messages that the SQL statement may result in
     */
    private ExpectedErrors expectedErrors;

    public OushuDBTableGenerator(String tableName, OushuDBSchema schema, OushuDBGlobalState globalState) {
        this.tableName = tableName;
        this.schema = schema;
        this.globalState = globalState;
        this.hasPrimaryKey = false;
        this.sqlBuilder = new StringBuilder();
        this.columns = new ArrayList<>();
        this.expectedErrors = new ExpectedErrors();
    }

    /**
     * @return SQL statement that create a new table
     */
    public SQLQueryAdapter generate() {
        sqlBuilder.append("CREATE");
        // define table storage format
        OushuDBTablespace tablespace = Randomly.fromList(schema.getAvailableTablespaces());
        tableStorage = Randomly.fromList(tablespace.getFilespaceType().getSupportedStorageTypes());
        // temporary or permanent
        if (Randomly.getBoolean()) {
            // create a temparary table
            tableType = OushuDBTable.TableType.TEMPORARY;
            // GLOBAL and LOCAL are for SQL standard compatibility, but have no effect in OushuDB.
            if (Randomly.getBoolean()) {
                sqlBuilder.append(Randomly.fromOptions(" GLOBAL", " LOCAL"));
            }
            sqlBuilder.append(Randomly.fromOptions(" TEMPARARY", " TEMP"));
        } else {
            tableType = OushuDBTable.TableType.BASE_TABLE;
        }
        sqlBuilder.append(" TABLE");
        if (Randomly.getBoolean()) {
            sqlBuilder.append(" IF NOT EXISTS");
        }
        sqlBuilder.append(' ');
        sqlBuilder.append(tableName);

        // generate table columns
        createStandardTable();
        // if (schema.getDatabaseTables().isEmpty() || Randomly.getBoolean()) {
        // createStandardTable();
        // } else {
        // createLikeTable();
        // assert(false);
        // }

        // define actions on commit for temporary tables
        if (tableType == OushuDBTable.TableType.TEMPORARY && Randomly.getBoolean()) {
            sqlBuilder.append(" ON COMMIT ");
            sqlBuilder.append(Randomly.fromOptions("PRESERVE ROWS", "DELETE ROWS", "DROP"));
            sqlBuilder.append(" ");
        }

        // append storage specifications
        appendStorage();

        // explicitly specify tablespace
        // TODO: ignore tablespace clause if in default ones
        sqlBuilder.append(" TABLESPACE ").append(tablespace.getTablespaceName());
        // TODO: support DISTRIBUTED BY and PARTITION BY

        return new SQLQueryAdapter(sqlBuilder.toString(), expectedErrors, true);
    }

    private void createStandardTable() {
        sqlBuilder.append(" (");
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            if (i != 0) {
                sqlBuilder.append(", ");
            }
            createColumn(DBMSCommon.createColumnName(i));
        }
        // TODO: support table constraints

        sqlBuilder.append(')');
        // TODO: support inherits
    }

    private void createColumn(String columnName) {
        sqlBuilder.append(columnName);
        sqlBuilder.append(" ");
        OushuDBDataType dataType;
        if (OushuDBProvider.generateOnlyKnown) {
            dataType = Randomly.fromList(OushuDBDataType.getKnownDataTypesByPQS());
        } else {
            dataType = Randomly.fromList(tableStorage.getSupportedDataTypes());
        }
        appendDataType(dataType);
        columns.add(new OushuDBColumn(columnName, dataType));
        // TODO: support column constraints
    }

    private void appendDataType(OushuDBDataType dataType) {
        switch (dataType) {
        case SMALLINT:
            sqlBuilder.append(Randomly.fromOptions("SMALLINT", "INT2"));
            break;
        case INTEGER:
            sqlBuilder.append(Randomly.fromOptions("INTEGER", "INT", "INT4"));
            break;
        case BIGINT:
            sqlBuilder.append(Randomly.fromOptions("BIGINT", "INT8"));
            break;
        case NUMERIC:
            // TODO: support (precision, scale)
            sqlBuilder.append(Randomly.fromOptions("NUMERIC", "DECIMAL"));
            break;
        case REAL:
            sqlBuilder.append(Randomly.fromOptions("REAL", "FLOAT"));
            break;
        case DOUBLE_PRECISION:
            sqlBuilder.append("DOUBLE PRECISION");
            break;
        case SMALLSERIAL:
            sqlBuilder.append("SMALLSERIA");
            break;
        case SERIAL:
            sqlBuilder.append("SERIAL");
            break;
        case BIGSERIAL:
            sqlBuilder.append("BIGSERIAL");
            break;
        case CHAR:
            sqlBuilder.append(Randomly.fromOptions("CHAR", "CHARACTER"));
            if (!Randomly.getBooleanWithSmallProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(1, Integer.MAX_VALUE));
                sqlBuilder.append(')');
            }
            break;
        case VARCHAR:
            sqlBuilder.append(Randomly.fromOptions("VARCHAR", "CHARACTER VARYING"));
            if (!Randomly.getBooleanWithSmallProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(1, Integer.MAX_VALUE));
                sqlBuilder.append(')');
            }
            break;
        case TEXT:
            sqlBuilder.append("TEXT");
            break;
        case BYTEA:
            sqlBuilder.append("BYTEA");
            break;
        case TIMESTAMP:
            sqlBuilder.append("TIMESTAMP");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(0, 6));
                sqlBuilder.append(')');
            }
            if (Randomly.getBoolean()) {
                sqlBuilder.append(" WITHOUT TIME ZONE");
            }
            break;
        case TIMESTAMPTZ:
            sqlBuilder.append("TIMESTAMP");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(0, 6));
                sqlBuilder.append(')');
            }
            sqlBuilder.append(" WITH TIME ZONE");
            break;
        case TIME:
            sqlBuilder.append("TIME");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(0, 6));
                sqlBuilder.append(')');
            }
            if (Randomly.getBoolean()) {
                sqlBuilder.append(" WITHOUT TIME ZONE");
            }
            break;
        case TIMETZ:
            sqlBuilder.append("TIME");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(0, 6));
                sqlBuilder.append(')');
            }
            sqlBuilder.append(" WITH TIME ZONE");
            break;
        case DATE:
            sqlBuilder.append("DATE");
            break;
        case INTERVAL:
            sqlBuilder.append("INTERVAL");
            // TODO: support [fileds]
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(0, 6));
                sqlBuilder.append(')');
            }
            break;
        case BOOLEAN:
            sqlBuilder.append("BOOLEAN");
            break;
        case POINT:
        case LSEG:
        case BOX:
        case PATH:
        case POLYGON:
        case CIRCLE:
            throw new IllegalArgumentException("geometric types are not supported yet");
        case CIDR:
        case INET:
        case MACADDR:
            throw new IllegalArgumentException("network address types are not supported yet");
        case BIT:
            sqlBuilder.append("BIT");
            if (!Randomly.getBooleanWithSmallProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(1, Integer.MAX_VALUE));
                sqlBuilder.append(')');
            }
            break;
        case VARBIT:
            sqlBuilder.append("BIT VARYING");
            if (!Randomly.getBooleanWithSmallProbability()) {
                sqlBuilder.append('(');
                sqlBuilder.append(globalState.getRandomly().getInteger(1, Integer.MAX_VALUE));
                sqlBuilder.append(')');
            }
            break;
        case ARRAY:
            throw new IllegalArgumentException("array types are not supported yet");
        default:
            throw new AssertionError("unknown data type: " + dataType.toString());
        }
    }

    // TODO: support CREATE TABLE LIKE
    @SuppressWarnings("unused")
    private void createLikeTable() {
    }

    private void appendStorage() {
        switch (tableStorage) {
        // HEAP, APPEND_ONLY, ORC, PARQUET, MAGMAAP, MAGMATP,
        // EXTERNAL, FOREIGN, HORC, HPARQUET, NONE
        case APPEND_ONLY:
            sqlBuilder.append(" WITH (APPENDONLY=TRUE, ORIENTATION=ROW)");
            // TODO: support more storage parameters
            // if (Randomly.getBoolean()) {
            // sqlBuilder.append(", BLOCKSIZE=");
            // sqlBuilder.append(globalState.getRandomly().getInteger(1, 256) * 8192);
            // }
            break;
        case ORC:
            // TODO: support ORC format parameters
            if (Randomly.getBoolean()) {
                sqlBuilder.append(" FORMAT 'ORC'");
            } else {
                sqlBuilder.append(" WITH (APPENDONLY=TRUE, ORIENTATION=ORC)");
            }
            break;
        case MAGMAAP:
            sqlBuilder.append("FORMAT 'MAGMAAP'");
            break;
        case EXTERNAL:
            // TEXT or CSV
            if (Randomly.getBoolean()) {
                sqlBuilder.append(" FORMAT 'TEXT'");
                // TODO: support TEXT format parameters
            } else {
                sqlBuilder.append(" FORMAT 'CSV'");
                // TODO: support CSV format parameters
            }
            if (Randomly.getBoolean()) {
                sqlBuilder.append("ENCODING '");
                // TODO: support more encodings
                sqlBuilder.append(Randomly.fromOptions("SQL-ASCII", "UTF-8", "GBK"));
                sqlBuilder.append("'");
            }
            break;
        case HEAP:
        case MAGMATP:
        case PARQUET:
        default:
            throw new IllegalArgumentException(
                    "unsupported table storage for generated tables: " + tableStorage.toString());
        }
    }

    public static SQLQueryAdapter generate(String tableName, OushuDBSchema schema, OushuDBGlobalState globalState) {
        return new OushuDBTableGenerator(tableName, schema, globalState).generate();
    }
}
