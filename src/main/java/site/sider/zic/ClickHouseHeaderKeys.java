package site.sider.zic;

import java.util.HashMap;
import java.util.Map;

public enum ClickHouseHeaderKeys {
    PROGRESS("X-ClickHouse-Progress"),
    QUERY_ID("X-ClickHouse-Query-Id"),
    FORMAT("X-ClickHouse-Format"),
    TIMEZONE("X-ClickHouse-Timezone"),
    PROGRESS_SUMMARY("X-ClickHouse-Summary"),
    USERNAME("X-ClickHouse-User"),
    PASSWORD("X-ClickHouse-Key");

    private String keyName;
    private ClickHouseHeaderKeys(String keyName) {
        this.keyName = keyName;
    }

    public static Map<String, ClickHouseHeaderKeys> keyMap = new HashMap<>();

    static {
        for (ClickHouseHeaderKeys key : ClickHouseHeaderKeys.values()) {
            keyMap.put(key.keyName, key);
        }
    }

    public static ClickHouseHeaderKeys getKey(String key) {
        return keyMap.get(key);
    }

    public String getKeyName() {
        return keyName;
    }
}
