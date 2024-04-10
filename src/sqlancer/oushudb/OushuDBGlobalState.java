package sqlancer.oushudb;

import sqlancer.SQLGlobalState;

public class OushuDBGlobalState extends SQLGlobalState<OushuDBOptions, OushuDBSchema> {

    @Override
    protected OushuDBSchema readSchema() throws Exception {
        return OushuDBSchema.fromConnection(getConnection(), getDatabaseName());
    }

}