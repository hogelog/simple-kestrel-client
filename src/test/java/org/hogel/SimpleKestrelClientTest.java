package org.hogel;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.net.Socket;

import org.junit.Test;

public class SimpleKestrelClientTest {

    @Test
    public void set_peek_get_peektimeout_gettimeout() throws Exception {
        Socket socket = new Socket("127.0.0.1", 22133);
        SimpleKestrelClient client = new SimpleKestrelClient(socket);

        client.set("hoge", "hoge\r\nhoge");

        assertThat(client.peek("hoge"), is("hoge\r\nhoge"));
        assertThat(client.get("hoge"), is("hoge\r\nhoge"));
        assertThat(client.get("hoge"), is(nullValue()));

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

    public void set_delte() throws Exception {
        Socket socket = new Socket("127.0.0.1", 22133);
        SimpleKestrelClient client = new SimpleKestrelClient(socket);

        client.set("hoge", "hogehoge");

        client.delete("hoge");
        assertThat(client.get("hoge"), is(nullValue()));
    }
}
