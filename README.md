# Zeppelin-Interpreter-for-ClickHouse

## Brief
1. Provide http based interpreter for ClickHouse
2. Compared with the JDBC interpreter of Zeppelin, this interpreter could query for a long time without timeout failure.

## How do deploy
1. Download the jar and put it in the path ${ZEPPELIN_HOME}/interpreter/clickhouse/
2. Restart Zeppelin.
3. Check the 'clickhouse' Interpreter in 'Interpreters' page of Zeppelin to ensure the interpreter is loaded successfully. 