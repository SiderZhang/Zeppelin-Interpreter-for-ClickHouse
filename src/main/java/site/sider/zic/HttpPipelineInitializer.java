package site.sider.zic;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.*;

import java.util.Map;
import java.util.function.Consumer;


public class HttpPipelineInitializer extends ChannelInitializer<Channel> {

    private Consumer<QueryResponse> queryResponseConsumer;
    private Connection connection;
    private String sql;
    private Map<String, String> querySetting;

    public HttpPipelineInitializer(Consumer<QueryResponse> queryResponseConsumer, Connection connection, String sql, Map<String, String> querySetting) {
        this.queryResponseConsumer = queryResponseConsumer;
        this.connection = connection;
        this.sql = sql;
        this.querySetting = querySetting;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new HttpRequestEncoder());
        pipeline.addLast(new ClickHouseHttpClient(queryResponseConsumer, connection, sql, querySetting));
    }
}
