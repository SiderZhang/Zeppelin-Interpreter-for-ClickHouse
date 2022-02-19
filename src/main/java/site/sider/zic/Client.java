package site.sider.zic;

import com.clickhouse.client.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Client {
    private Logger logger = LoggerFactory.getLogger(Client.class);

    private static final String DEFAULT_KEY = "default";
    private static final String DOT = ".";

    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String USER_KEY = "user";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_KEY = "database";


    private static final String DEFAULT_HOST_KEY = DEFAULT_KEY + DOT + HOST_KEY;
    private static final String DEFAULT_PORT_KEY = DEFAULT_KEY + DOT + PORT_KEY;
    private static final String DEFAULT_USER_KEY = DEFAULT_KEY + DOT + USER_KEY;
    private static final String DEFAULT_PASSWORD_KEY = DEFAULT_KEY + DOT + PASSWORD_KEY;
    private static final String DEFAULT_DATABASE_KEY = DEFAULT_KEY + DOT + DATABASE_KEY;

    private ClickHouseNode server;

    public Client(Properties properties) {
        ClickHouseNode.Builder builder = ClickHouseNode.builder();

        if (properties.containsKey(DEFAULT_DATABASE_KEY)) {
            String database = properties.getProperty(DEFAULT_DATABASE_KEY);
            if (StringUtils.isNoneBlank(database)) {
                builder.database(database);
            }
        }

        if (properties.containsKey(DEFAULT_HOST_KEY)) {
            String host = properties.getProperty(DEFAULT_HOST_KEY);
            if (StringUtils.isNoneBlank(host)) {
                builder.host(host);
            }
        }

        if (properties.containsKey(DEFAULT_PORT_KEY)) {
            String portStr = properties.getProperty(DEFAULT_PORT_KEY);
            if (StringUtils.isNumeric(portStr)) {
                Integer port = Integer.parseInt(portStr);
                builder.port(port);
            } else {
                builder.port(ClickHouseProtocol.HTTP);
            }
        }

        if (properties.containsKey(DEFAULT_USER_KEY) ||
                properties.containsKey(DEFAULT_PASSWORD_KEY)) {
            String user = properties.getProperty(DEFAULT_USER_KEY, "");
            String password = properties.getProperty(DEFAULT_PASSWORD_KEY, "");
            builder.credentials(ClickHouseCredentials.fromUserAndPassword(user, password));
        }

        // connect to localhost, use default port of the preferred protocol
        server = builder.build();
    }

    public String executeSQL(String sql, String queryId) throws ExecutionException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();

        // connect to localhost, use default port of the preferred protocol
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            ClickHouseRequest<?> req = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            req.session(UUID.randomUUID().toString(), false);
            req.getQueryId();

            try (ClickHouseResponse resp = req.query("set send_progress_in_http_headers=1").execute().get()) {
                resp.getSummary();
            } catch (ExecutionException | InterruptedException e) {
                logger.error("get exception while setting" , e);
                throw e;
            }

            try (ClickHouseResponse resp = req.query(sql, queryId).execute().get()) {
                Iterator<ClickHouseColumn> columnIterator = resp.getColumns().iterator();
                while (columnIterator.hasNext()) {
                    ClickHouseColumn column = columnIterator.next();
                    stringBuilder.append(column.getColumnName());
                    if (columnIterator.hasNext()) {
                        stringBuilder.append("\t");
                    } else {
                        stringBuilder.append("\n");
                    }
                }

                // or resp.stream() if you prefer stream API
                for (ClickHouseRecord record : resp.records()) {
                    Iterator<ClickHouseValue> valueIter = record.iterator();
                    while (valueIter.hasNext()) {
                        ClickHouseValue value = valueIter.next();
                        stringBuilder.append(value.asString());
                        if (valueIter.hasNext()) {
                            stringBuilder.append("\t");
                        } else {
                            stringBuilder.append("\n");
                        }
                    }
                }

                ClickHouseResponseSummary summary = resp.getSummary();
                ClickHouseResponseSummary.Progress progress = summary.getProgress();
                logger.info("progress " + progress.getReadRows() + ", " + progress.getTotalRowsToRead());
            } catch (ExecutionException | InterruptedException e) {
                logger.error("got exception while querying: " + sql, e);
                throw e;
            }
        }

        return stringBuilder.toString();
    }
}
