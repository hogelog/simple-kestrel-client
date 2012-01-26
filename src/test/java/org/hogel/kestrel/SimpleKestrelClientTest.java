package org.hogel.kestrel;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleKestrelClientTest {
    private SimpleKestrelClient client;

    @Before
    public void before() throws IOException {
        Socket socket = new Socket("127.0.0.1", 22133);
        client = new SimpleKestrelClient(socket);
        client.delete("hoge");
    }

    @After
    public void after() throws IOException {
        client.delete("hoge");
        client.close();
    }

    @Test
    public void set_peek_get() throws Exception {
        client.set("hoge", "hoge\r\nhoge");

        assertThat(client.peek("hoge"), is("hoge\r\nhoge"));
        assertThat(client.get("hoge"), is("hoge\r\nhoge"));
        assertThat(client.get("hoge"), is(nullValue()));
    }

    @Test
    public void timeout_set_peek_get() throws Exception {
        new Thread(){
            @Override
            public void run() {
                try {
                    Socket socket = new Socket("127.0.0.1", 22133);
                    SimpleKestrelClient client = new SimpleKestrelClient(socket);
                    Thread.sleep(1000);
                    client.set("hoge", "hogefuga");
                    Thread.sleep(1000);
                    client.set("hoge", "hogemoge");
                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        assertThat(client.peek("hoge", 5000), is("hogefuga"));
        assertThat(client.get("hoge"), is("hogefuga"));
        assertThat(client.get("hoge"), is(nullValue()));
        assertThat(client.get("hoge", 5000), is("hogemoge"));
    }

    @Test
    public void set_delete() throws Exception {
        client.set("hoge", "hogehoge");

        assertThat(client.peek("hoge"), is("hogehoge"));

        client.delete("hoge");
        assertThat(client.get("hoge"), is(nullValue()));
    }

    @Test
    public void many_set() throws Exception {
        for (int i = 0; i < 40000; ++i) {
            client.set("hoge", "hogehoge");
        }
        client.delete("hoge");
    }
}
