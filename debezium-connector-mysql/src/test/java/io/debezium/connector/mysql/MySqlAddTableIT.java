/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.util.Testing;

public class MySqlAddTableIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Files.createTestingPath("file-db-history-schema-parameter.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("schemaparameterit", "source_type_as_schema_parameter_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void schemaTest1() throws SQLException, InterruptedException {

        Testing.Files.delete(DB_HISTORY_PATH);
        String table1 = "table1";
        String table2 = "table2";
        String create1 = String.format("CREATE TABLE %s (id INT, name VARCHAR(30));", table1);
        String create2 = String.format("CREATE TABLE %s (id INT, name VARCHAR(30));", table2);
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(create1);
                connection.execute(create2);
            }
        }

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(table1))
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(DatabaseHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(table1))).isNullOrEmpty();
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(5);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into table1(id, name) values(11, 'a1');");
            }
        }

        records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(table1)).size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName())).isNullOrEmpty();
        stopConnector();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into table1(id, name) values(12, 'a2');");
            }
        }

        config = config.edit()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST.name(), String.format("%s,%s", DATABASE.qualifiedTableName(table1), DATABASE.qualifiedTableName(table2)))
                .build();

        start(MySqlConnector.class, config);

        records = consumeRecordsByTopic(20);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(table1)).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(table2))).isNullOrEmpty();
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(7);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into table2(id, name) values(21, 'b1');");
            }
        }

        records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(table1))).isNullOrEmpty();
        assertThat(records.recordsForTopic(DATABASE.topicForTable(table2)).size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName())).isNullOrEmpty();

        stopConnector();
    }

}
