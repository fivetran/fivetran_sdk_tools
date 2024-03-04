package testers.util;

import static testers.util.SdkConverters.valueTypeToDataType;

import com.google.common.annotations.VisibleForTesting;
import fivetran_sdk.Checkpoint;
import fivetran_sdk.Column;
import fivetran_sdk.DataType;
import fivetran_sdk.OpType;
import fivetran_sdk.Operation;
import fivetran_sdk.Record;
import fivetran_sdk.Schema;
import fivetran_sdk.SchemaChange;
import fivetran_sdk.Table;
import fivetran_sdk.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class MockConnectorOutput implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(MockConnectorOutput.class.getName());

    private final Map<OpType, Consumer<Record>> recordMapper = new EnumMap<>(OpType.class);

    private final MockWarehouse destination;
    private final String defaultSchema;
    private final Consumer<String> stateSaver;
    private final Supplier<String> stateLoader;

    private final Map<SchemaTable, Map<String, Column>> tableDefinitions = new HashMap<>();

    private static final Pattern LONG_MATCHER = Pattern.compile("[+-]?\\d+");
    private static final Pattern DOUBLE_MATCHER =
            Pattern.compile("[+-]?(?:(?:\\d*\\.\\d+(?:[eE][+-]?\\d+)?)|(?:\\d+\\.\\d*))");

    private long upsertCount = 0;
    private long updateCount = 0;
    private long deleteCount = 0;
    private long truncateCount = 0;
    private long checkpointCount = 0;
    private long schemaChangeCount = 0;
    private String latestState = null;

    public MockConnectorOutput(
            MockWarehouse destination,
            String defaultSchema,
            Consumer<String> stateSaver,
            Supplier<String> stateLoader) {
        this.destination = destination;
        this.defaultSchema = defaultSchema;
        this.stateSaver = stateSaver;
        this.stateLoader = stateLoader;

        recordMapper.put(OpType.UPSERT, this::handleUpsert);
        recordMapper.put(OpType.UPDATE, this::handleUpdate);
        recordMapper.put(OpType.DELETE, this::handleDelete);
        recordMapper.put(OpType.TRUNCATE, this::handleTruncate);
    }

    public void enqueueOperation(Operation op) {
        Operation.OpCase opCase = op.getOpCase();
        switch (opCase) {
            case RECORD:
                Record record = op.getRecord();
                recordMapper.get(record.getType()).accept(record);
                break;

            case SCHEMA_CHANGE:
                SchemaChange schemaChange = op.getSchemaChange();
                SchemaChange.ChangeCase changeCase = schemaChange.getChangeCase();
                if (changeCase == SchemaChange.ChangeCase.WITH_SCHEMA) {
                    for (Schema schema : schemaChange.getWithSchema().getSchemasList()) {
                        for (Table table : schema.getTablesList()) {
                            handleSchemaChange(schema.getName(), table);
                        }
                    }
                } else if (changeCase == SchemaChange.ChangeCase.WITHOUT_SCHEMA) {
                    for (Table table : schemaChange.getWithoutSchema().getTablesList()) {
                        handleSchemaChange(defaultSchema, table);
                    }
                }
                break;

            case CHECKPOINT:
                handleCheckpoint(op.getCheckpoint());
                break;

            default:
                throw new RuntimeException("Unrecognized operation: " + opCase);
        }
    }

    private void handleUpsert(Record record) {
        upsertCount++;
        SchemaTable schemaTable =
                new SchemaTable(record.hasSchemaName() ? record.getSchemaName() : defaultSchema, record.getTableName());
        Map<String, ValueType> dataMap = record.getDataMap();

        handleColumnChanges(schemaTable, dataMap);

        destination.upsert(schemaTable, tableDefinitions.get(schemaTable).values(), dataMap);
        LOG.info(String.format("[Upsert]: %s  Data: %s", schemaTable, dataMap));
    }

    private void handleUpdate(Record record) {
        updateCount++;
        SchemaTable schemaTable =
                new SchemaTable(record.hasSchemaName() ? record.getSchemaName() : defaultSchema, record.getTableName());
        Map<String, ValueType> dataMap = record.getDataMap();

        handleColumnChanges(schemaTable, dataMap);

        destination.update(schemaTable, tableDefinitions.get(schemaTable).values(), dataMap);
        LOG.info(String.format("[Update]: %s  Data: %s", schemaTable, dataMap));
    }

    private void handleDelete(Record record) {
        deleteCount++;
        SchemaTable schemaTable =
                new SchemaTable(record.hasSchemaName() ? record.getSchemaName() : defaultSchema, record.getTableName());
        Map<String, ValueType> dataMap = record.getDataMap();
        destination.delete(schemaTable, tableDefinitions.get(schemaTable).values(), dataMap);
        LOG.info(String.format("[Delete]: %s  Data: %s", schemaTable, dataMap));
    }

    private void handleTruncate(Record record) {
        truncateCount++;
        SchemaTable schemaTable =
                new SchemaTable(record.hasSchemaName() ? record.getSchemaName() : defaultSchema, record.getTableName());
        destination.truncate(schemaTable);
        LOG.info(String.format("[Truncate]: %s", schemaTable));
    }

    @VisibleForTesting
    public static DataType mergeTypes(DataType incoming, @Nullable DataType destination) {
        if (destination == null) {
            return incoming;
        }

        if (incoming.getNumber() > destination.getNumber()) {
            if (incoming.getNumber() <= DataType.DOUBLE.getNumber()) {
                return incoming;
            } else if (incoming.getNumber() <= DataType.UTC_DATETIME.getNumber()) {
                if (destination.getNumber() >= DataType.NAIVE_DATE.getNumber()) {
                    return incoming;
                } else {
                    return DataType.STRING;
                }
            } else {
                return DataType.STRING;
            }

        } else if (destination.getNumber() > incoming.getNumber()) {
            if (destination.getNumber() <= DataType.DOUBLE.getNumber()) {
                return destination;
            } else if (destination.getNumber() <= DataType.UTC_DATETIME.getNumber()) {
                if (incoming.getNumber() >= DataType.NAIVE_DATE.getNumber()) {
                    return destination;
                } else {
                    return DataType.STRING;
                }
            } else {
                return DataType.STRING;
            }
        }

        return destination;
    }

    private DataType inferType(ValueType valueType) {
        if (valueType.hasString()) {
            String value = valueType.getString();
            if (isInstant(value)) return DataType.UTC_DATETIME;
            if (isLocalDateTime(value)) return DataType.NAIVE_DATETIME;
            if (isLocalDate(value)) return DataType.NAIVE_DATE;
            if (isBoolean(value)) return DataType.BOOLEAN;
            if (isLong(value)) return DataType.LONG;
            if (isDouble(value)) return DataType.DOUBLE;
            return DataType.STRING;
        } else {
            return valueTypeToDataType(valueType);
        }
    }

    private boolean isBoolean(@Nonnull String value) {
        String lower = value.toLowerCase();
        return lower.equals("true") || lower.equals("false") || lower.equals("t") || lower.equals("f");
    }

    private boolean isDouble(@Nonnull String value) {
        return value.equals("NaN") || value.equals("Infinity") || DOUBLE_MATCHER.matcher(value).matches();
    }

    private boolean isLong(@Nonnull String value) {
        try {
            if (!LONG_MATCHER.matcher(value).matches()) return false;
            if ((value.startsWith("0") && !value.equals("0")) || value.startsWith("+")) return false;
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isLocalDate(@Nonnull String value) {
        try {
            LocalDate.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return  false;
        }
    }

    private boolean isInstant(@Nonnull String value) {
        try {
            Instant.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private boolean isLocalDateTime(@Nonnull String value) {
        try {
            LocalDateTime.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private void handleColumnChanges(SchemaTable schemaTable, Map<String, ValueType> dataMap) {
        boolean createTable = false;
        boolean tableExists = destination.exists(schemaTable);
        Map<String, Column> knownColumns =
                tableDefinitions.getOrDefault(schemaTable, new HashMap<>());

        for (String incomingColumnName : dataMap.keySet()) {
            DataType knownType = knownColumns
                    .getOrDefault(incomingColumnName, Column.newBuilder().build()).getType();
            DataType incomingType = knownType != DataType.UNSPECIFIED ?
                    knownType : inferType(dataMap.get(incomingColumnName));
            DataType destinationType =  destination.getColumnType(schemaTable, incomingColumnName).orElse(null);
            DataType mergedType = mergeTypes(incomingType, destinationType);

            if (mergedType.getNumber() != knownType.getNumber()) {
                if (knownColumns.containsKey(incomingColumnName)) {
                    if (tableExists) {
                        destination.changeColumnType(schemaTable, incomingColumnName, mergedType);
                    } else {
                        createTable = true;
                    }

                    updateKnownColumn(schemaTable, knownColumns, incomingColumnName, mergedType);
                } else {
                    if (tableExists) {
                        destination.addColumn(schemaTable, incomingColumnName, mergedType);
                    } else {
                        createTable = true;
                    }

                    addKnownColumn(schemaTable, incomingColumnName, incomingType);
                }
            }
        }

        if (createTable) {
            destination.createTable(schemaTable, tableDefinitions.get(schemaTable).values());
        }
    }

    private void addKnownColumn(SchemaTable schemaTable, String columnName, DataType newDataType) {
        Column newColumn = Column.newBuilder().setName(columnName).setPrimaryKey(false).setType(newDataType).build();
        if (!tableDefinitions.containsKey(schemaTable)) {
            tableDefinitions.put(schemaTable, new HashMap<>());
        }

        tableDefinitions.get(schemaTable).put(columnName, newColumn);
    }

    private void updateKnownColumn(
            SchemaTable schemaTable, Map<String, Column> definedColumns, String columnName, DataType newDataType) {
        Column newColumn =
                Column.newBuilder()
                        .setName(columnName)
                        .setType(newDataType)
                        .setPrimaryKey(definedColumns.get(columnName).getPrimaryKey())
                        //.setDecimal()  // TODO
                        .build();
        definedColumns.put(columnName, newColumn);
        tableDefinitions.get(schemaTable).put(columnName, newColumn);
    }

    public void handleSchemaChange(String schema, Table table) {
        schemaChangeCount++;
        LOG.info(String.format("[SchemaChange]: %s.%s", schema, table.getName()));

        SchemaTable schemaTable = new SchemaTable(schema, table.getName());
        if (tableDefinitions.containsKey(schemaTable)) {
            if (tableDefinitions.get(schemaTable).equals(table.getColumnsList())) {
                // No change in table
                return;
            }

            // ALTER existing table
            // TODO: Possibilities: 1. add column, 2. change type (compare against existing Table)

        } else {
            if (table.getColumnsList().stream().noneMatch(c -> c.getPrimaryKey() && c.getType() == DataType.UNSPECIFIED)) {
                // Create table as long as it does not have any PK columns with UNSPECIFIED data type
                List<Column> specifiedColumns =
                        table.getColumnsList().stream()
                                .filter(c -> c.getType() != DataType.UNSPECIFIED).collect(Collectors.toList());
                if (!specifiedColumns.isEmpty()) {
                    destination.createTable(schemaTable, specifiedColumns);
                }
            }

            Map<String, Column> columns =
                    table.getColumnsList().stream().filter(c -> c.getType() != DataType.UNSPECIFIED)
                            .collect(Collectors.toMap(Column::getName, Function.identity()));
            tableDefinitions.put(schemaTable, columns);
        }
    }

    private void handleCheckpoint(Checkpoint checkpoint) {
        checkpointCount++;

        String newStateJson = checkpoint.getStateJson();
        stateSaver.accept(newStateJson);
        latestState = newStateJson;
        LOG.info("Checkpoint: " + latestState);
    }

    // Core is responsible for saving state, why would it not be able to provide the latest state to a connector?!
    public String getState() {
        if (latestState == null) {
            latestState = stateLoader.get();
        }

        return latestState;
    }

    public void displayReport() {
        LOG.info(
                String.format(
                        "Upserts: %d\nUpdates: %d\nDeletes: %d\nTruncates: %d\nSchemaChanges: %d\nCheckpoints: %d",
                        upsertCount, updateCount, deleteCount, truncateCount, schemaChangeCount, checkpointCount));
    }

    @Override
    public void close() {}
}
