package org.hogel.kestrel;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.ClientConfig.Yes;
import com.twitter.finagle.kestrel.java.Client;
import com.twitter.finagle.kestrel.protocol.Command;
import com.twitter.finagle.kestrel.protocol.Kestrel;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.util.Duration;
import com.twitter.util.Time;

public class SimpleKestrelClient implements Closeable {

    private final Client client;

    public SimpleKestrelClient(String host, int port) {
        this(new InetSocketAddress(host, port));
    }

    public SimpleKestrelClient(InetSocketAddress addr) {
        ClientBuilder<Command, Response, Yes, Yes, Yes> builder = ClientBuilder.get()
                    .codec(Kestrel.get())
                    .hosts(addr)
                    .hostConnectionLimit(1);
        ServiceFactory<Command, Response> kestrelClientBuilder = ClientBuilder.safeBuildFactory(builder);
        client = Client.newInstance(kestrelClientBuilder);
    }

    public void delete(String queueName) {
        client.delete(queueName).apply();
    }

    @Override
    public void close() {
        client.close();
    }

    public void set(String queueName, String value) {
        set(queueName, 0, value);
    }

    public void set(String queueName, int exp, String value) {
        Time expTime = Time.fromMilliseconds(exp);
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(value.getBytes(CharsetUtil.UTF_8));
        client.set(queueName, buffer, expTime);
    }

    public String get(String queueName) {
        return get(queueName, 0);
    }

    public String get(String queueName, int waitFor) {
        Duration waitDuration = Duration.apply(waitFor, TimeUnit.MILLISECONDS);
        return get(queueName, waitDuration);
    }

    public String get(String queueName, Duration waitDuration) {
        ChannelBuffer value = client.get(queueName, waitDuration).apply();
        return value == null ? null : value.toString(CharsetUtil.UTF_8);
    }

    public String peek(String queueName) {
        return peek(queueName, 0);
    }

    public String peek(String queueName, int waitFor) {
        Duration waitDuration = Duration.apply(waitFor, TimeUnit.MILLISECONDS);
        return peek(queueName, waitDuration);
    }

    public String peek(String queueName, Duration waitDuration) {
        return get(queueName + "/peek", waitDuration);
    }
}
