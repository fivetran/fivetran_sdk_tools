package testers;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static testers.SdkConnectorTester.CONFIG_FILE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import client.connector.SdkConnectorClient;
import client.destination.SdkWriterClient;
import fivetran_sdk.*;
import testers.util.InstantFormattedSerializer;
import testers.util.SdkCrypto;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import picocli.CommandLine;

/** This tool mocks Fivetran connector + core for testing SDK Destination */
public final class SdkDestinationTester {
    private static final Logger LOG = Logger.getLogger(SdkDestinationTester.class.getName());

    private static final String VERSION = "024.0410.001";

    private static final CsvMapper CSV = createCsvMapper();
    private static final String DEFAULT_SCHEMA = "tester";
    private static final String DEFAULT_NULL_STRING = "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP";
    private static final String DEFAULT_UPDATE_UNMODIFIED = "unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3";
    private static final String SYNCED_SYS_COLUMN = "_fivetran_synced";
    private static final String DELETED_SYS_COLUMN = "_fivetran_deleted";
    private static final String ID_SYS_COLUMN = "_fivetran_id";

    private SdkDestinationTester() {}

    @CommandLine.Command(name = "cliargs", mixinStandardHelpOptions = true, description = "Command line args")
    public static class CliArgs {
        @CommandLine.Option(
                names = {"--working-dir"},
                required = true,
                description = "Directory to use for reading/writing files")
        String workingDir;

        @CommandLine.Option(
                names = {"--port"},
                defaultValue = "50052",
                required = true,
                description = "")
        String port;

        @CommandLine.Option(
                names = {"--schema-name"},
                defaultValue = DEFAULT_SCHEMA,
                description = "Use a custom schema name")
        String schemaName;

        @CommandLine.Option(
                names = {"--input-file"},
                defaultValue = "",
                description = "Use the input file passed in to generate a batch file")
        String inputFile;

        @CommandLine.Option(
                names = {"--plain-text"},
                description = "Disable encryption and compression")
        boolean plainText = false;
    }

    public static void main(String[] args) throws IOException {
        CliArgs cliargs = new CliArgs();
        new CommandLine(cliargs).parseArgs(args);

        String grpcHost =
                (System.getenv("GRPC_HOSTNAME") == null)
                        ? SdkConnectorClient.DEFAULT_GRPC_HOST
                        : System.getenv("GRPC_HOSTNAME");

        String grpcWorkingDir = (System.getenv("WORKING_DIR") == null) ?
                cliargs.workingDir : System.getenv("WORKING_DIR");
        int grpcPort = Integer.parseInt(cliargs.port);
        new SdkDestinationTester().run(grpcWorkingDir, grpcHost, grpcPort, cliargs);
    }

    public void run(String grpcWorkingDir, String grpcHost, int grpcPort, CliArgs cliargs) throws IOException {
        LOG.info("Version: " + VERSION);
        LOG.info("GRPC_HOSTNAME: " + grpcHost);
        LOG.info("GRPC_PORT: " + grpcPort);
        LOG.info("Working Directory: " + grpcWorkingDir);
        LOG.info("Schema name: " + cliargs.schemaName);
        if (!cliargs.inputFile.isEmpty()) {
            LOG.info("Input file: " + cliargs.inputFile);
        }
        LOG.info("NULL string: " + DEFAULT_NULL_STRING);
        LOG.info("UNMODIFIED string: " + DEFAULT_UPDATE_UNMODIFIED);
        LOG.info("Compression: " + ((cliargs.plainText) ? "NONE" : "ZSTD"));
        LOG.info("Encryption: " + ((cliargs.plainText) ? "NONE" : "AES/CBC"));

        ManagedChannel channel = SdkWriterClient.createChannel(grpcHost, grpcPort);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> SdkWriterClient.closeChannel(channel)));
        SdkWriterClient client = new SdkWriterClient(channel);

        File directoryPath = new File(cliargs.workingDir);
        File[] filesList;
        if (!cliargs.inputFile.isEmpty()) {
            filesList = new File[]{ Paths.get(cliargs.workingDir, cliargs.inputFile).toFile() };
        } else {
            filesList = directoryPath.listFiles();
            if (filesList == null) {
                LOG.severe("ERROR: Directory is empty");
                System.exit(1);
            }
        }

        LOG.info("Fetching configuration form");
        Path configFilePath = Paths.get(cliargs.workingDir, CONFIG_FILE);
        ConfigurationFormResponse configurationForm = client.configurationForm();
        SdkConnectorTester.saveConfig(configurationForm, configFilePath);

        String strConfig = Files.readString(configFilePath);
        LOG.info("Configuration:\n" + strConfig);
        Map<String, String> creds =
                SdkConnectorTester.JSON.readValue(strConfig, new SdkConnectorTester.MapTypeReference());

        LOG.info("Running setup tests");
        for (ConfigurationTest connectorTest : configurationForm.getTestsList()) {
            Optional<String> testResponse = client.test(connectorTest.getName(), creds);
            String result = testResponse.orElse("PASSED");
            System.out.println("[" + connectorTest.getLabel() + "]: " + result);
            if (!testResponse.isEmpty()) {
                LOG.severe("Exiting due to test failure!");
                System.exit(1);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        for (File file : filesList) {
            if (file.isFile() && !file.getName().equals(CONFIG_FILE) && file.getName().endsWith(".json")) {
                LinkedHashMap<String, Object> batch = mapper.readValue(file, new TypeReference<>() {});
                String filename = file.getName().replaceFirst("[.][^.]+$", "");
                executeInputFile(
                        cliargs.schemaName, filename, batch, client, cliargs.workingDir, grpcWorkingDir, creds, cliargs.plainText);
            }
        }

        System.exit(0);
    }

    /** Executes the elements of the input file in the same order as Fivetran core */
    private void executeInputFile(
            String schemaName,
            String batchName,
            Map<String, Object> batch,
            SdkWriterClient client,
            String workingDir,
            String grpcWorkingDir,
            Map<String, String> config,
            boolean plainText)
            throws IOException {
        Map<String, Table> tables = new HashMap<>();
        // <schema-table, Map<op-type, rows>>
        Map<String, Map<String, List<Object>>> tableDMLs = new HashMap<>();

        // describeTable
        if (batch.containsKey("describe_table")) {
            Object entry = batch.get("describe_table");

            if (!(entry instanceof Collection<?>)) {
                throw new RuntimeException("Describe_Table should have a list of table name(s)");
            }

            for (String tableName : (List<String>) entry) {
                DescribeTableResponse response = client.describeTable(schemaName, tableName, config);

                if (response.hasFailure()) {
                    throw new RuntimeException(
                            String.format("Failed to fetch table `%s`: %s", tableName, response.getFailure()));
                } else if (response.getNotFound()) {
                    LOG.info(String.format("Table does not exist at the destination: %s", tableName));
                } else {
                    if (!tableDMLs.containsKey(tableName)) {
                        tableDMLs.put(tableName, new LinkedHashMap<>());
                    }

                    Table table = response.getTable();
                    tables.put(tableName, table);
                    LOG.info(String.format("Describe Table: %s\n%s", tableName, table));
                }
            }
        }

        // createTable (needs to be fully defined)
        if (batch.containsKey("create_table")) {
            for (var tableEntry : ((Map<String, Object>) batch.get("create_table")).entrySet()) {
                String tableName = tableEntry.getKey();
                if (tables.containsKey(tableName)) {
                    LOG.fine("Table already exists: " + tableName);
                }

                if (tableDMLs.containsKey(tableName)) {
                    LOG.warning("Replacing definition for table: " + tableName);
                }
                tableDMLs.put(tableName, new LinkedHashMap<>());

                Table table = buildTable(tableName, (Map<String, Object>) tableEntry.getValue());

                Optional<String> result = client.createTable(schemaName, table, config);
                if (result.isPresent()) {
                    throw new RuntimeException(result.get());
                } else {
                    tables.put(tableName, table);
                    LOG.info(String.format("Create Table succeeded: %s", tableName));
                }
            }
        }

        // alterTable (needs to be fully defined)
        if (batch.containsKey("alter_table")) {
            for (var tableEntry : ((Map<String, Object>) batch.get("alter_table")).entrySet()) {
                String tableName = tableEntry.getKey();

                if (tableDMLs.containsKey(tableName)) {
                    LOG.info("Updating definition for table: " + tableName);
                }
                tableDMLs.put(tableName, new LinkedHashMap<>());

                Table table = buildTable(tableName, (Map<String, Object>) tableEntry.getValue());

                Optional<String> result = client.alterTable(schemaName, table, config);
                if (result.isPresent()) {
                    throw new RuntimeException(result.get());
                } else {
                    tables.put(tableName, table);
                    LOG.info(String.format("Alter Table succeeded: %s", tableName));
                }
            }
        }

        // --- Upsert, Update, Delete, Truncate (with timestamp)
        // <schema-table, timestamp>
        Map<String, Instant> softTruncateBefores = new HashMap<>();
        // <schema-table, timestamp>
        Map<String, Instant> hardTruncateBefores = new HashMap<>();
        if (batch.containsKey("ops")) {
            separateOpsToTables(
                    client,
                    schemaName,
                    config,
                    tables,
                    (List<Map<String, Object>>) batch.get("ops"),
                    tableDMLs,
                    softTruncateBefores,
                    hardTruncateBefores);

            // Create batch files per table
            for (var entry : tableDMLs.entrySet()) {
                String table = entry.getKey();
                // At this point we should have a Table object for each table in the ops
                if (!tables.containsKey(table)) {
                    throw new RuntimeException(
                            String.format("Table definition is missing: '%s'", table));
                }
                Map<String, List<Object>> tableDML = entry.getValue();
                List<Column> columns = tables.get(table).getColumnsList();

                Map<String, ByteString> keys = new HashMap<>();
                List<String> replaceList = new ArrayList<>();
                List<String> updateList = new ArrayList<>();
                List<String> deleteList = new ArrayList<>();

                // separate batch file per op-type
                for (var entry2 : tableDML.entrySet()) {
                    String opName = entry2.getKey().toLowerCase();
                    List<Object> rows = entry2.getValue();
                    SecretKey key = SdkCrypto.newEphemeralKey();
                    String extension = (plainText) ? "csv" : "csv.zstd.aes";
                    String filename = String.format("%s_%s_%s.%s", table, batchName, opName, extension);
                    Path path = Paths.get(workingDir, filename);
                    CsvSchema csvSchema = buildCsvSchema(shuffle(columns));
                    writeFile(path, key, csvSchema, opName, columns, rows, table, plainText);
                    Path grpcPath = Paths.get(grpcWorkingDir, filename);
                    keys.put(grpcPath.toString(), ByteString.copyFrom(key.getEncoded()));
                    switch (opName) {
                        case "upsert" -> replaceList.add(grpcPath.toString());
                        case "update" -> updateList.add(grpcPath.toString());
                        case "delete" -> deleteList.add(grpcPath.toString());
                    }
                }

                // Send batch files first
                client.writeBatch(
                        config,
                        schemaName,
                        table,
                        columns,
                        replaceList,
                        updateList,
                        deleteList,
                        keys,
                        "CSV_ZSTD",
                        DEFAULT_NULL_STRING,
                        DEFAULT_UPDATE_UNMODIFIED);
                LOG.info(String.format("WriteBatch succeeded: %s", table));

                // Then truncate if any
                if (softTruncateBefores.containsKey(table)) {
                    Instant ts = softTruncateBefores.get(table);
                    LOG.info(String.format("Truncating: %s [%s]", table, ts.toString()));
                    client.truncate(
                            schemaName,
                            table,
                            DELETED_SYS_COLUMN,
                            SYNCED_SYS_COLUMN,
                            ts,
                            true,
                            config);
                    LOG.info(String.format("Truncate succeeded: %s", table));
                }

                // Then hard-truncate if any
                if (hardTruncateBefores.containsKey(table)) {
                    Instant ts = hardTruncateBefores.get(table);
                    LOG.info(String.format("Hard Truncating: %s [%s]", table, ts.toString()));
                    client.truncate(
                            schemaName,
                            table,
                            DELETED_SYS_COLUMN,
                            SYNCED_SYS_COLUMN,
                            ts,
                            false,
                            config);
                    LOG.info(String.format("Hard Truncate succeeded: %s", table));
                }
            }
        }
    }

    private void writeFile(
            Path path,
            SecretKey key,
            CsvSchema csvSchema,
            String opName,
            List<Column> columns,
            List<Object> rows,
            String table,
            boolean plainText)
            throws IOException {
        try (OutputStream out = createOutputStream(path, key, plainText);
                SequenceWriter sw =
                        CSV.writer(csvSchema).writeValues(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {

            Set<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toSet());
            for (var row : rows) {
                Map<String, Object> data = (Map<String, Object>) row;

                if (data.containsKey(ID_SYS_COLUMN) && !columnNames.contains(ID_SYS_COLUMN)) {
                    throw new RuntimeException(
                            String.format(
                                    "System column '%s' is found in op '%s' for table: %s",
                                    ID_SYS_COLUMN, opName, table));
                }

                if (!data.containsKey(ID_SYS_COLUMN) && columnNames.contains(ID_SYS_COLUMN)) {
                    throw new RuntimeException(
                            String.format(
                                    "Column '_fivetran_id' is missing in op '%s' for pkeyless table: %s",
                                    opName, table));
                }

                if (columnNames.contains(DELETED_SYS_COLUMN)) {
                    if (opName.equalsIgnoreCase("upsert")) {
                        data.put(DELETED_SYS_COLUMN, false);
                    } else if (opName.equalsIgnoreCase("update")) {
                        if (!data.containsKey(DELETED_SYS_COLUMN)) {
                            data.put(DELETED_SYS_COLUMN, false);
                        }
                    }
                }

                for (var c : columns) {
                    if (!data.containsKey(c.getName())) {
                        if (opName.equalsIgnoreCase("upsert")) {
                            throw new RuntimeException(
                                    String.format(
                                            "Column '%s' is missing in op '%s' for table: %s",
                                            c.getName(), opName, table));
                        } else if (opName.equalsIgnoreCase("update")) {
                            data.put(c.getName(), DEFAULT_UPDATE_UNMODIFIED);
                        } else if (opName.equalsIgnoreCase("delete")) {
                            data.put(c.getName(), null);
                        } else {
                            throw new RuntimeException("Unrecognized op: " + opName);
                        }
                    }
                }

                sw.write(data);
            }

            sw.flush();
        }
    }

    private OutputStream createOutputStream(Path path, SecretKey secretKey, boolean plainText) throws IOException {
        OutputStream outputStream = Files.newOutputStream(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        if (plainText) {
            return outputStream;
        }
        CipherOutputStream cipherStream = SdkCrypto.encryptWrite(outputStream, secretKey);
        return new ZstdOutputStream(cipherStream, -5);
    }

    private List<Column> shuffle(List<Column> incoming) {
        List<Column> columns = new ArrayList<>(incoming);
        Collections.shuffle(columns);
        return columns;
    }

    private CsvSchema buildCsvSchema(List<Column> columns) {
        CsvSchema.Builder builder = CsvSchema.builder();
        for (Column c : columns) {
            builder.addColumn(c.getName(), csvType(c.getType()));
        }
        return builder.setUseHeader(true).setNullValue(DEFAULT_NULL_STRING).build();
    }

    private Table buildTable(String table, Map<String, Object> tableEntry) {
        Table.Builder tableBuilder = Table.newBuilder().setName(table);
        List<Column> columns = new ArrayList<>();

        List<String> pkeys =
                (tableEntry.containsKey("primary_key"))
                        ? (List<String>) tableEntry.get("primary_key")
                        : Collections.emptyList();

        if (!tableEntry.containsKey("columns")) {
            throw new RuntimeException("Table definition does not contain any columns");
        }

        for (var columnEntry : ((Map<String, Object>) tableEntry.get("columns")).entrySet()) {
            String columnName = columnEntry.getKey();

            if (columnName.equals(DELETED_SYS_COLUMN) ||
                    columnName.equals(SYNCED_SYS_COLUMN) ||
                    columnName.equals(ID_SYS_COLUMN)) {
                throw new RuntimeException(String.format("%s is a Fivetran system column name", columnName));
            }

            Column.Builder columnBuilder = Column.newBuilder().setName(columnName);
            Object dataType = columnEntry.getValue();

            if (dataType instanceof String dataTypeStr) {
                columnBuilder.setType(strToDataType(dataTypeStr.toUpperCase()));
            } else if (dataType instanceof Map) {
                // Currently this is only for DECIMAL data type
                Map<String, Object> dataTypeMap = (Map<String, Object>) dataType;
                String strDataType = ((String) dataTypeMap.get("type")).toUpperCase();
                if (!strDataType.equals("DECIMAL")) {
                    throw new RuntimeException("Expecting DECIMAL data type");
                }
                int precision = (int) dataTypeMap.get("precision");
                int scale = (int) dataTypeMap.get("scale");
                columnBuilder
                        .setType(DataType.DECIMAL)
                        .setDecimal(DecimalParams.newBuilder()
                        .setPrecision(precision)
                        .setScale(scale)
                        .build());
            } else {
                throw new RuntimeException("Unexpected data type object");
            }

            if (pkeys.contains(columnName)) {
                columnBuilder.setPrimaryKey(true);
            }

            columns.add(columnBuilder.build());
        }

        // Add system columns
        columns.add(Column.newBuilder()
                .setName(SYNCED_SYS_COLUMN)
                .setType(DataType.UTC_DATETIME)
                .build());
        if (pkeys.isEmpty()) {
            columns.add(Column.newBuilder()
                    .setName(ID_SYS_COLUMN)
                    .setType(DataType.STRING)
                    .setPrimaryKey(true)
                    .build());
        }

        return tableBuilder.addAllColumns(columns).build();
    }

    private CsvSchema.ColumnType csvType(DataType type) {
        switch (type) {
            case BOOLEAN:
                return CsvSchema.ColumnType.BOOLEAN;
            case STRING:
            case NAIVE_DATE:
            case NAIVE_DATETIME:
            case UTC_DATETIME:
            case NAIVE_TIME:
            case JSON:
            case UNSPECIFIED:
            case BINARY:
            case XML:
                return CsvSchema.ColumnType.STRING;
            case SHORT:
            case INT:
            case LONG:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return CsvSchema.ColumnType.NUMBER;
            default:
                throw new RuntimeException("Unknown type " + type);
        }
    }

    private DataType strToDataType(String type) {
        try {
            return DataType.valueOf(type);
        } catch (Exception e) {
            throw new RuntimeException("Unsupported data type: " + type);
        }
    }

    private void separateOpsToTables(
            SdkWriterClient client,
            String schemaName,
            Map<String, String> config,
            Map<String, Table> tables,
            List<Map<String, Object>> ops,
            Map<String, Map<String, List<Object>>> tableDMLs,
            Map<String, Instant> softTruncateBefores,
            Map<String, Instant> hardTruncateBefores) {

        for (var opEntry : ops) {
            if (opEntry.size() > 1) {
                throw new RuntimeException("Each operation entry should have a single operation in it");
            }
            String opName = opEntry.keySet().toArray()[0].toString();
            Object op = opEntry.values().toArray()[0];

            if (opName.equalsIgnoreCase("upsert") ||
                    opName.equalsIgnoreCase("update") ||
                    opName.equalsIgnoreCase("soft_delete") ||
                    opName.equalsIgnoreCase("delete")) {
                for (var entry2 : ((Map<String, Object>) op).entrySet()) {
                    String table = entry2.getKey();
                    if (!tableDMLs.containsKey(table)) {
                        throw new RuntimeException("Unknown table: " + table);
                    }

                    List<Object> rows = (List<Object>) entry2.getValue();

                    // Add system columns as needed
                    for (var row : rows) {
                        Map<String, Object> rowData = (Map<String, Object>) row;
                        if (((Map<?, ?>) row).containsKey(SYNCED_SYS_COLUMN)) {
                            throw new RuntimeException(SYNCED_SYS_COLUMN + " is a system column");
                        }
                        rowData.put(SYNCED_SYS_COLUMN, Instant.ofEpochMilli(System.currentTimeMillis()));

                        if (((Map<?, ?>) row).containsKey(DELETED_SYS_COLUMN)) {
                            throw new RuntimeException(DELETED_SYS_COLUMN + " is a system column");
                        }

                        if (opName.equalsIgnoreCase("soft_delete")) {
                            rowData.put(DELETED_SYS_COLUMN, true);
                            if (!containsDeletedSysColumn(tables.get(table))) {
                                Table newTable = addDeletedSysColumn(tables.get(table));
                                tables.put(table, newTable);

                                client.alterTable(schemaName, newTable, config);
                            }
                        }

                        // Add a tiny bit of delay to simulate a real sync
                        delay();
                    }

                    Map<String, List<Object>> tableDML = tableDMLs.get(table);
                    String remappedOpName =
                            (opName.equalsIgnoreCase("soft_delete")) ? "update" : opName;
                    if (tableDML.containsKey(remappedOpName)) {
                        tableDML.get(remappedOpName).addAll(rows);
                    } else {
                        tableDML.put(remappedOpName, new ArrayList<>(rows));
                    }
                }

            } else if (opName.equalsIgnoreCase("soft_truncate_before")) {
                if (!(op instanceof Collection<?>)) {
                    throw new RuntimeException("SoftTruncateBefore should have a list of table name(s)");
                }

                for (String table : (Collection<String>) op) {
                    if (softTruncateBefores.containsKey(table)) {
                        LOG.fine("Another soft_truncate_before for table: " + table);
                    }

                    softTruncateBefores.put(table, Instant.ofEpochMilli(System.currentTimeMillis()));
                    if (!containsDeletedSysColumn(tables.get(table))) {
                        Table newTable = addDeletedSysColumn(tables.get(table));
                        tables.put(table, newTable);

                        client.alterTable(schemaName, newTable, config);
                    }
                }
            } else if (opName.equalsIgnoreCase("truncate_before")) {
                if (!(op instanceof Collection<?>)) {
                    throw new RuntimeException("TruncateBefore should have a list of table name(s)");
                }

                for (String table : (Collection<String>) op) {
                    if (softTruncateBefores.containsKey(table)) {
                        LOG.fine("Another truncate_before for table: " + table);
                    }

                    hardTruncateBefores.put(table, Instant.ofEpochMilli(System.currentTimeMillis()));
                }

            } else {
                throw new RuntimeException(
                        "Unexpected entry: " + opName + " | " + op.toString() + " | " + op.getClass());
            }

            // Add a tiny bit of delay to simulate a real sync
            delay();
        }
    }

    private Table addDeletedSysColumn(Table table) {
        List<Column> columns = new ArrayList<>(table.getColumnsList());
        columns.add(Column.newBuilder()
                .setName(DELETED_SYS_COLUMN)
                .setType(DataType.BOOLEAN)
                .build());
        return Table.newBuilder()
                .setName(table.getName())
                .addAllColumns(columns)
                .build();
    }

    private boolean containsDeletedSysColumn(Table table) {
        for (Column c : table.getColumnsList()) {
            if (c.getName().equalsIgnoreCase(DELETED_SYS_COLUMN)) {
                return true;
            }
        }
        return false;
    }

    private static void delay() {
        try {
            TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static CsvMapper createCsvMapper() {
        CsvMapper mapper = new CsvMapper();

        // Set up time serializers
        JavaTimeModule dates = new JavaTimeModule();
        DateTimeFormatter format =
                new DateTimeFormatterBuilder()
                        .appendValue(YEAR, 4, 10, SignStyle.NEVER)
                        .appendLiteral('-')
                        .appendValue(MONTH_OF_YEAR, 2)
                        .appendLiteral('-')
                        .appendValue(DAY_OF_MONTH, 2)
                        .toFormatter();

        dates.addSerializer(LocalDate.class, new LocalDateSerializer(format));
        dates.addSerializer(Instant.class, new InstantFormattedSerializer(DateTimeFormatter.ISO_INSTANT));

        mapper.registerModule(dates);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_EMPTY_STRINGS, true);

        // Jackson CSV needs this for some reason
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        return mapper;
    }
}
