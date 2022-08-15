package site.sider.zic;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class QueryRequest implements Consumer<QueryResponse> {
    private Logger logger = LoggerFactory.getLogger(QueryRequest.class);

    private QueryResponse curResponse = null;

    private Connection connection;

    private Map<String, String> querySettings;

    public QueryRequest(Connection connection, Map<String, String> querySettings) {
        this.connection = connection;
        this.querySettings = querySettings;
    }
    public void executeSQL(String sql) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(connection.getINetSocketAddress())
                .handler(new HttpPipelineInitializer(this, connection, sql, querySettings));

        ChannelFuture f = b.connect().sync();
        f.channel().closeFuture().sync();

        group.shutdownGracefully().sync();
    }

    public QueryResponse getResponse() {
        return this.curResponse;
    }

    public String getResult() {
        return StringUtils.join(this.curResponse.getBodyList(), "\n");
    }

    @Override
    public void accept(QueryResponse queryResponse) {
        this.curResponse = queryResponse;
    }
}
