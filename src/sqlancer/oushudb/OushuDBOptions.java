package sqlancer.oushudb;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.oushudb.OushuDBOptions.OushuDBOracleFactory;
import sqlancer.postgres.PostgresOptions;

public class OushuDBOptions implements DBMSSpecificOptions<OushuDBOracleFactory> {

    public enum OushuDBOracleFactory implements OracleFactory<OushuDBGlobalState> {
        PQS {

            @Override
            public TestOracle<OushuDBGlobalState> create(OushuDBGlobalState globalState) throws SQLException {
                return null;
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        };

    }
    public static final String DEFAULT_HOST = "localhost";

    public static final int DEFAULT_PORT = 5432;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<OushuDBOracleFactory> oracle = Arrays.asList(OushuDBOracleFactory.PQS);

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = String.format("postgresql://%s:%d/test", PostgresOptions.DEFAULT_HOST,
            PostgresOptions.DEFAULT_PORT);

    @Override
    public List<OushuDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}