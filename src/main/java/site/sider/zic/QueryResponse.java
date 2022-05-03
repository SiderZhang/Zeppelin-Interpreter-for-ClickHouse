package site.sider.zic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

@Data
public class QueryResponse {
    private static Logger LOGGER = LoggerFactory.getLogger(QueryResponse.class);

    private String queryId;
    private String format;
    private String timezone;
    private long totalRows;
    private long readRows;
    private int retCode = -1;

    private List<String> bodyList = new LinkedList<>();

    public void onHeader(String headerLine) {

        if (retCode == -1) {
            parseStatusLine(headerLine);
            return;
        }

        int index = headerLine.indexOf(':');
        if (index == -1) {
            LOGGER.error("invalid header line " + headerLine);
            return;
        }

        String key = headerLine.substring(0, index).trim();
        String value = headerLine.substring(index + 1).trim();

        ClickHouseHeaderKeys clickHouseHeaderKey = ClickHouseHeaderKeys.getKey(key);
        if (clickHouseHeaderKey == null) {
            LOGGER.error("unsupported header: " + headerLine.trim());
            return;
        }

        switch (clickHouseHeaderKey) {
            case FORMAT:
                format = value;
                break;
            case PROGRESS:
            case PROGRESS_SUMMARY:
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    Progress progress = objectMapper.readValue(value, Progress.class);
                    totalRows = progress.getTotal_rows_to_read();
                    readRows = progress.getRead_rows();
                } catch (JsonProcessingException e) {
                    LOGGER.error("failed to parse progress data " + headerLine, e);
                }
                break;
            case QUERY_ID:
                queryId = value;
                break;
            case TIMEZONE:
                timezone = value;
                break;
            default:
                LOGGER.error("unsupported header: " + headerLine.trim());
        }

        LOGGER.debug("content: <" + headerLine.trim() + ">");
    }

    private void parseStatusLine(String line) {
        String[] args = line.split(" ");
        retCode = Integer.parseInt(args[1]);
    }

    public void onBody(String bodyLine) {
        bodyList.add(bodyLine);
        LOGGER.debug("content: <{}>", bodyLine.trim());
    }
}
