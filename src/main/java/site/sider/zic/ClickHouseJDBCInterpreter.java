package site.sider.zic;

import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import org.apache.zeppelin.interpreter.*;

import java.util.Properties;
import java.util.UUID;

public class ClickHouseJDBCInterpreter extends Interpreter {
    private Client client;

    public ClickHouseJDBCInterpreter(Properties properties) {
        super(properties);
        client = new Client(properties);
    }

    @Override
    public void open() throws InterpreterException {

    }

    @Override
    public void close() throws InterpreterException {

    }

    @Override
    public InterpreterResult interpret(String s, InterpreterContext interpreterContext) throws InterpreterException {
        String queryId = UUID.randomUUID().toString();
        String result = client.executeSQL(s, queryId);
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, result);
    }

    @Override
    public void cancel(InterpreterContext interpreterContext) throws InterpreterException {

    }

    @Override
    public FormType getFormType() throws InterpreterException {
        return null;
    }

    @Override
    public int getProgress(InterpreterContext interpreterContext) throws InterpreterException {
        return 0;
    }
}
