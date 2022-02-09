package site.sider.zic;

import com.clickhouse.client.*;
import com.google.common.collect.Streams;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Client {
    private Logger logger = LoggerFactory.getLogger(Client.class);

    private ClickHouseNode server;

    public Client(Properties properties) {
        ClickHouseNode.Builder builder = ClickHouseNode.builder();

        if (properties.containsKey("database")) {
            String database = properties.getProperty("database");
            if (StringUtils.isNoneBlank(database)) {
                builder.database(database);
            }
        }

        if (properties.containsKey("port")) {
            String portStr = properties.getProperty("port");
            if (StringUtils.isNumeric(portStr)) {
                Integer port = Integer.parseInt(portStr);
                builder.port(port);
            } else {
                builder.port(ClickHouseProtocol.HTTP);
            }
        }

        if (properties.containsKey("host")) {
            String host = properties.getProperty("host");
            if (StringUtils.isNoneBlank(host)) {
                builder.host(host);
            }
        }

        // connect to localhost, use default port of the preferred protocol
        server = builder.build();
    }

    public String executeSQL(String sql, String queryId) {
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
            }
        }

        return stringBuilder.toString();
    }
}
