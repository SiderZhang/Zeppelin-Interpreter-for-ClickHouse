package site.sider.zic;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.*;
import io.netty.util.ByteProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.util.List;
import java.util.function.Consumer;

public class ClickHouseHttpClient extends ByteToMessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHttpInterpreter.class);

    private Boolean headOver = false;

    private QueryResponse queryResponse = new QueryResponse();

    private Consumer<QueryResponse> consumer = null;
    private Connection connection = null;
    private String sql = null;

    public ClickHouseHttpClient(Consumer<QueryResponse> consumer, Connection connection, String sql) {
        this.consumer = consumer;
        this.sql = sql;
        this.connection = connection;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        URI url = new URI("/?send_progress_in_http_headers=1&query=" + URLEncoder.encode(sql, "UTF-8"));
        //配置HttpRequest的请求数据和一些配置信息
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_0, HttpMethod.GET, url.toASCIIString());

        request.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain;charset=UTF-8")
                //开启长连接
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
                //设置传递请求内容的长度
                .set(HttpHeaderNames.CONTENT_LENGTH, request.content().readableBytes())
                .set(ClickHouseHeaderKeys.FORMAT.getKeyName(), "TabSeparatedWithNames");

        if (StringUtils.isNoneBlank(connection.getUsername())) {
            request.headers()
                    .set(ClickHouseHeaderKeys.USERNAME.getKeyName(), connection.getUsername())
                    .set(ClickHouseHeaderKeys.PASSWORD.getKeyName(), connection.getPassword());
        }

        //发送数据
        ctx.writeAndFlush(request);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        if (!in.isReadable()) {
            return;
        }

        int index = in.forEachByte(ByteProcessor.FIND_LF);
        if (index == -1) {
            return;
        }

        int startIndex = in.readerIndex();

        byte[] buffer = new byte[index + 1 - startIndex];
        in.readBytes(buffer);
        String data = new String(buffer);
        data = data.trim();

        if (data.length() == 0 && !headOver) {
            headOver = true;
            return;
        }

        if (!headOver) {
            queryResponse.onHeader(data);
        } else {
            queryResponse.onBody(data);
        }
        out.add(data);
        if (consumer != null) {
            consumer.accept(queryResponse);
        }
        LOGGER.debug("packet got <{}> : {}", data.trim(), System.currentTimeMillis());
    }

    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        super.decodeLast(ctx, in, out);

        if (in.isReadable()) {
            int size = in.writerIndex() - in.readerIndex();
            byte[] buffer = new byte[size];
            String data = new String(buffer);
            data = data.trim();

            if (!headOver) {
                queryResponse.onHeader(data);
            } else {
                queryResponse.onBody(data);
            }
        }

        if (consumer != null) {
            consumer.accept(queryResponse);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
