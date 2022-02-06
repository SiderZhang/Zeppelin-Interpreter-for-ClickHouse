package site.sider.zic;

import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.Properties;

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
        client.executeSQL(s);
        return new InterpreterResult(InterpreterResult.Code.SUCCESS);
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
