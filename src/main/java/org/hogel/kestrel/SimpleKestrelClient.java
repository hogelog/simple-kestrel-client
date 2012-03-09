package org.hogel.kestrel;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class SimpleKestrelClient implements Closeable {
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static enum ResponseType {
        VALUE,
        END,
        STORED,
        CLIENT_ERROR,
        DELETED,
        ;

        static final Map<String, ResponseType> typesMap;
        static {
            ResponseType[] values = values();
            typesMap = new HashMap<String, ResponseType>(values.length);
            for (ResponseType responseType : values) {
                typesMap.put(responseType.name(), responseType);
            }
        }

        public static ResponseType responseType(String code) {
            String rawCode = code.substring(0, code.length() - 2);
            if (typesMap.containsKey(rawCode)) {
                return typesMap.get(rawCode);
            }
            if (rawCode.startsWith("VALUE ")) {
                return VALUE;
            }
            throw new IllegalArgumentException("unknown response type code: " + rawCode);
        }
    }

    private final Socket socket;
    private final BufferedInputStream input;
    private final BufferedOutputStream output;
    private final KestrelCommandFactory commandFactory;

    public SimpleKestrelClient(Socket socket) throws UnknownHostException, IOException {
        this.socket = socket;
        input = new BufferedInputStream(socket.getInputStream());
        output = new BufferedOutputStream(socket.getOutputStream());
        commandFactory = new KestrelCommandFactory();
    }

    static final byte[] CRLF = "\r\n".getBytes(CHARSET_UTF8);
    private void send(String command) throws IOException {
        send(command.getBytes(CHARSET_UTF8));
    }

    private void send(byte[] data) throws IOException {
        send(data, false);
    }

    private void send(byte[] data, boolean withCRLF) throws IOException {
        output.write(data);
        if (withCRLF) {
            output.write(CRLF);
        }
        output.flush();
    }

    private byte[] recv() throws IOException {
        ByteArrayOutputStream dataBuffer = new ByteArrayOutputStream();
        int prevch = -1, ch;
        while ((ch = input.read()) != -1) {
            dataBuffer.write(ch);
            if (ch == '\n' && prevch == '\r') {
                break;
            }
            prevch = ch;
        }
        return dataBuffer.toByteArray();
    }

    private ResponseType recvResponseType() throws IOException {
        String response = new String(recv(), CHARSET_UTF8);
        return ResponseType.responseType(response);
    }

    private byte[] recvData(int length) throws IOException {
        byte[] data = new byte[length];
        if (input.read(data) != length) {
            throw new IllegalStateException("Recieve Data Terminated");
        }
        int cr = input.read(), lf = input.read();
        if (cr != '\r' || lf != '\n') {
            if (cr == -1 || lf == -1) {
                throw new IllegalStateException("No terminate code");
            } else {
                throw new IllegalStateException(String.format("Invalid terminate code %02x%02x", cr, lf));
            }
        }
        return data;
    }

    public void set(String key, String value) throws IOException {
        set(key, 0, value);
    }

    public void set(String key, int expiration, String value) throws IOException {
        byte[] valueData = value.getBytes(CHARSET_UTF8);
        String command = commandFactory.setCommand(key, expiration, valueData);
        send(command);
        send(valueData, true);

        ResponseType responseType = recvResponseType();
        if (responseType != ResponseType.STORED) {
            throw new IllegalArgumentException(responseType.name());
        }
    }

    static final Pattern SPACE_PATTERN = Pattern.compile("\\s");
    public String rawGet(String command) throws IOException {
        send(command);
        String getResponseTypeCode = new String(recv(), CHARSET_UTF8);
        ResponseType getResponseType = ResponseType.responseType(getResponseTypeCode);
        if (getResponseType == ResponseType.END) {
            return null;
        }
        if (getResponseType != ResponseType.VALUE) {
            throw new IllegalArgumentException(getResponseType.name());
        }

        int length = Integer.valueOf(SPACE_PATTERN.split(getResponseTypeCode)[3]);
        byte[] valueData = recvData(length);
        String value = new String(valueData, CHARSET_UTF8);

        ResponseType endResponseType = recvResponseType();
        if (endResponseType != ResponseType.END) {
            throw new IllegalArgumentException(endResponseType.name());
        }
        return value;
    }

    public String get(String key) throws IOException {
        String command = commandFactory.getCommand(key);
        return rawGet(command);
    }

    public String get(String key, long timeout) throws IOException {
        String command = commandFactory.getCommand(key, timeout);
        return rawGet(command);
    }

    public String peek(String key) throws IOException {
        String command = commandFactory.peekCommand(key);
        return rawGet(command);
    }

    public String peek(String key, long timeout) throws IOException {
        String command = commandFactory.peekCommand(key, timeout);
        return rawGet(command);
    }

    public void delete(String key) throws IOException {
        String command = commandFactory.deleteCommand(key);
        send(command);
        ResponseType deletedResponseType = recvResponseType();
        if (deletedResponseType != ResponseType.DELETED && deletedResponseType != ResponseType.END) {
            throw new IllegalArgumentException(deletedResponseType.name());
        }
    }

    public void close() throws IOException {
        socket.close();
    }
}
