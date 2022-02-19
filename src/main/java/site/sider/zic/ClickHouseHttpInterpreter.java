package site.sider.zic;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.Properties;
import java.util.UUID;

public class ClickHouseHttpInterpreter extends Interpreter {
    private Client client;

    public ClickHouseHttpInterpreter(Properties properties) {
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
        try {
            String result = client.executeSQL(s, queryId);
            return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, result);
        } catch (Exception e) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
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
        return 0;
    }
}
