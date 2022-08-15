package site.sider.zic;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClickHouseHttpInterpreter extends Interpreter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHttpInterpreter.class);

    private Map<String, QueryRequest> queryRequestMap = new ConcurrentHashMap<>();

    private static final String DEFAULT_KEY = "default";
    private static final String DOT = ".";

    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String USER_KEY = "user";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_KEY = "database";
    private static final String QUERY_SETTING = "settings.query";

    private static final String DEFAULT_HOST_KEY = DEFAULT_KEY + DOT + HOST_KEY;
    private static final String DEFAULT_PORT_KEY = DEFAULT_KEY + DOT + PORT_KEY;
    private static final String DEFAULT_USER_KEY = DEFAULT_KEY + DOT + USER_KEY;
    private static final String DEFAULT_PASSWORD_KEY = DEFAULT_KEY + DOT + PASSWORD_KEY;
    private static final String DEFAULT_DATABASE_KEY = DEFAULT_KEY + DOT + DATABASE_KEY;
    private static final String QUERY_SETTING_PREFIX = DEFAULT_KEY + DOT + QUERY_SETTING;

    private Connection connection;

    public ClickHouseHttpInterpreter(Properties properties) {
        super(properties);
    }

    @Override
    public void open() throws InterpreterException {
        int port = 0;
        String portStr = properties.getProperty(DEFAULT_PORT_KEY);
        if (StringUtils.isNumeric(portStr)) {
            port = Integer.parseInt(portStr);
        }

        String user = properties.getProperty(DEFAULT_USER_KEY, "");
        String password = properties.getProperty(DEFAULT_PASSWORD_KEY, "");

        connection =
                Connection
                .builder()
                .database(properties.getProperty(DEFAULT_DATABASE_KEY))
                .host(properties.getProperty(DEFAULT_HOST_KEY))
                .port(port)
                .username(user)
                .password(password)
                .build();
    }

    @Override
    public void close() throws InterpreterException {

    }

    @Override
    public InterpreterResult interpret(String s, InterpreterContext interpreterContext) throws InterpreterException {
        String paraId = interpreterContext.getParagraphId();
        QueryRequest queryRequest = queryRequestMap.get(paraId);

        // Existing query;
        if (Objects.nonNull(queryRequest)) {
            return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, queryRequest.getResult());
        }

        Map<String, String> querySettingMap = properties.stringPropertyNames()
                .stream()
                .filter(name -> name.startsWith(QUERY_SETTING_PREFIX))
                .collect(Collectors.toMap(key -> key, key -> properties.getProperty(key, "")));

        try {
            queryRequest = new QueryRequest(connection, querySettingMap);
            queryRequestMap.put(paraId, queryRequest);
            queryRequest.executeSQL(s);
            int retCode = queryRequest.getResponse().getRetCode();
            if (retCode < 200 || retCode > 300) {
                String errResult = queryRequest.getResult();
                interpreterContext.out.write(errResult);
                interpreterContext.out.flush();
                return new InterpreterResult(InterpreterResult.Code.ERROR);
            }

            String result = queryRequest.getResult();
            return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, result);
        } catch (Exception e) {
            try {
                interpreterContext.out().setType(InterpreterResult.Type.NULL);
                interpreterContext.out().write(ExceptionUtils.getStackTrace(e));
                interpreterContext.out.flush();
            } catch (IOException ex) {
                LOGGER.error("failed to write output", ex);
            }

            return new InterpreterResult(InterpreterResult.Code.ERROR);
        } finally {
            if (queryRequestMap != null) {
                queryRequestMap.remove(paraId);
            }
        }
    }

    @Override
    public void cancel(InterpreterContext interpreterContext) throws InterpreterException {

    }

    @Override
    public FormType getFormType() throws InterpreterException {
        return FormType.SIMPLE;
    }

    @Override
    public int getProgress(InterpreterContext interpreterContext) throws InterpreterException {
        String paraId = interpreterContext.getParagraphId();
        QueryRequest queryRequest = queryRequestMap.get(paraId);
        QueryResponse queryResponse = queryRequest.getResponse();
        long read = queryResponse.getReadRows();
        long total = queryResponse.getTotalRows();
        if (total == 0) {
            return 0;
        }
        int result = (int) (read * 100.0 / total);
        result = Math.max(result, 0);
        result = Math.min(result, 100);
        return result;
    }
}
