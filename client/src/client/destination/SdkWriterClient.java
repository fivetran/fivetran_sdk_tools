package client.destination;

import static client.connector.SdkConnectorClient.MAX_MESSAGE_SIZE;
import static client.connector.SdkConnectorClient.waitForServer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import fivetran_sdk.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SdkWriterClient {
    private static final Duration WRITE_BATCH_TIMEOUT = Duration.of(1, ChronoUnit.HOURS);

    private final ManagedChannel channel;
    private DestinationConnectorGrpc.DestinationConnectorBlockingStub blockingStub = null;

    public SdkWriterClient(ManagedChannel channel) {
        this.channel = channel;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public ConfigurationFormResponse configurationForm() {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();
        ConfigurationFormRequest configFormRequest = ConfigurationFormRequest.newBuilder().build();
        return conn.configurationForm(configFormRequest);
    }

    public CapabilitiesResponse capabilities() {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();
        CapabilitiesRequest capabilitiesRequest = CapabilitiesRequest.newBuilder().build();
        return conn.capabilities(capabilitiesRequest);
    }

    public Optional<String> test(String testName, Map<String, String> config) {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();
        TestRequest request = TestRequest.newBuilder().setName(testName).putAllConfiguration(config).build();

        TestResponse response = conn.test(request);

        TestResponse.ResponseCase responseCase = response.getResponseCase();

        if (responseCase == TestResponse.ResponseCase.SUCCESS) {
            return Optional.empty();
        } else if (responseCase == TestResponse.ResponseCase.FAILURE) {
            return Optional.of(response.getFailure());
        }

        throw new RuntimeException("Unknown test response: " + responseCase);
    }

    public DescribeTableResponse describeTable(String schema, String table, Map<String, String> config) {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();
        DescribeTableRequest.Builder requestBuilder =
                DescribeTableRequest.newBuilder().putAllConfiguration(config).setSchemaName(schema).setTableName(table);
        return conn.describeTable(requestBuilder.build());
    }

    public Optional<String> createTable(String schema, Table table, Map<String, String> config) {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();
        CreateTableRequest request =
                CreateTableRequest.newBuilder()
                        .putAllConfiguration(config)
                        .setSchemaName(schema)
                        .setTable(table)
                        .build();

        CreateTableResponse response = conn.createTable(request);
        return response.hasFailure() ? Optional.of(response.getFailure()) : Optional.empty();
    }

    public Optional<String> alterTable(String schema, Table table, List<SchemaDiff> schemaDiffs, Map<String, String> config) {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();

        AlterTableRequest request =
                AlterTableRequest.newBuilder()
                        .putAllConfiguration(config)
                        .setSchemaName(schema)
                        .setTableName(table.getName())
                        .addAllChanges(schemaDiffs)
                        .build();

        AlterTableResponse response = conn.alterTable(request);
        return response.hasFailure() ? Optional.of(response.getFailure()) : Optional.empty();
    }

    public TruncateResponse truncate(
            String schema,
            String table,
            String deletedColumn,
            String syncedColumn,
            Instant deleteBefore,
            boolean soft,
            Map<String, String> config) {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();

        TruncateRequest.Builder requestBuilder =
                TruncateRequest.newBuilder()
                        .putAllConfiguration(config)
                        .setSchemaName(schema)
                        .setTableName(table)
                        .setSyncedColumn(syncedColumn)
                        .setUtcDeleteBefore(
                                Timestamp.newBuilder()
                                        .setSeconds(deleteBefore.getEpochSecond())
                                        .setNanos(deleteBefore.getNano())
                                        .build());

        if (soft) {
            requestBuilder.setSoft(SoftTruncate.newBuilder().setDeletedColumn(deletedColumn).build());
        }

        return conn.truncate(requestBuilder.build());
    }

    public WriteBatchResponse writeBatch(
            Map<String, String> config,
            String schemaName,
            String tableName,
            List<Column> columns,
            List<String> replace,
            List<String> update,
            List<String> delete,
            Map<String, ByteString> keys,
            String fileFormat,
            String nullString,
            String unmodifiedString) {
        DestinationConnectorGrpc.DestinationConnectorBlockingStub conn = getBlockingStub();

        Table table = Table.newBuilder().setName(tableName).addAllColumns(columns).build();

        WriteBatchRequest.Builder requestBuilder =
                WriteBatchRequest.newBuilder()
                        .putAllConfiguration(config)
                        .setSchemaName(schemaName)
                        .setTable(table)
                        .putAllKeys(keys)
                        .addAllReplaceFiles(replace)
                        .addAllUpdateFiles(update)
                        .addAllDeleteFiles(delete);

        // Delimiter = ,
        // Quote = "
        // Escape = "
        // Date/Timestamp format = ISO 8601	(2019-02-11T05:09:12.195Z)
        // Header = yes
        if (fileFormat.equals("CSV_ZSTD")) {
            requestBuilder.setCsv(
                    CsvFileParams.newBuilder()
                            .setCompression(Compression.ZSTD)
                            .setEncryption(Encryption.AES)
                            .setNullString(nullString)
                            .setUnmodifiedString(unmodifiedString)
                            .build());
        } else {
            throw new RuntimeException("Unsupported batch file format");
        }

        return conn.withDeadlineAfter(WRITE_BATCH_TIMEOUT.toMinutes(), TimeUnit.MINUTES)
                .writeBatch(requestBuilder.build());
    }

    private DestinationConnectorGrpc.DestinationConnectorBlockingStub getBlockingStub() {
        if (blockingStub == null) {
            waitForServer(channel);
            blockingStub = DestinationConnectorGrpc.newBlockingStub(channel)
                    .withCompression("gzip")
                    .withWaitForReady();
        }
        return blockingStub;
    }

    public static ManagedChannel createChannel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .build();
    }

    public static void closeChannel(ManagedChannel channel) {
        try {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
