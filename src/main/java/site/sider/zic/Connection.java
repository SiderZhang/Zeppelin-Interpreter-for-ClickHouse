package site.sider.zic;

import lombok.Builder;
import lombok.Data;

import java.net.InetSocketAddress;

@Builder
@Data
public class Connection {
    private String username;
    private String password;
    private String host;
    private int port;
    private String database;

    public InetSocketAddress getINetSocketAddress() {
        return new InetSocketAddress(host, port);
    }
}
