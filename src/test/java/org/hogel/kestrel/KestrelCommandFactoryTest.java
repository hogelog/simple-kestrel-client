package org.hogel.kestrel;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.charset.Charset;

import org.junit.Before;
import org.junit.Test;

public class KestrelCommandFactoryTest {

    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private KestrelCommandFactory factory;

    @Before
    public void before() {
        factory = new KestrelCommandFactory();
    }

    @Test
    public void test_set() throws Exception {
        assertThat(factory.setCommand("hoge", "fuga".getBytes(CHARSET_UTF8)), is("set hoge 0 0 4\r\n"));
        assertThat(factory.setCommand("hoge", 1000, "fuga".getBytes(CHARSET_UTF8)), is("set hoge 0 1000 4\r\n"));
    }

    @Test
    public void test_get() throws Exception {
        assertThat(factory.getCommand("hoge"), is("get hoge\r\n"));
        assertThat(factory.peekCommand("hoge"), is("get hoge/peek\r\n"));
        assertThat(factory.getCommand("hoge", 1000), is("get hoge/t=1000\r\n"));
        assertThat(factory.peekCommand("hoge", 1000), is("get hoge/peek/t=1000\r\n"));
    }

    @Test
    public void test_delete() throws Exception {
        assertThat(factory.deleteCommand("hoge"), is("delete hoge\r\n"));
    }

}
