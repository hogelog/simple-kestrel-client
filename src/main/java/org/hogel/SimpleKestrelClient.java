package org.hogel;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class SimpleKestrelClient {
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static enum ResponseType {
        VALUE,
        END,
        STORED,
        CLIENT_ERROR,
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

    private final BufferedInputStream input;
    private final BufferedOutputStream output;

    public SimpleKestrelClient(Socket socket) throws UnknownHostException, IOException {
        input = new BufferedInputStream(socket.getInputStream());
        output = new BufferedOutputStream(socket.getOutputStream());
    }

    static final byte[] CRLF = "\r\n".getBytes(CHARSET_UTF8);
    private void send(String command) throws IOException {
        send(command.getBytes(CHARSET_UTF8));
    }

    private void send(byte[] data) throws IOException {
        output.write(data);
        output.write(CRLF);
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
        for (int i = 0; i < length; ) {
            byte[] recvData = recv();
            int recvLength = recvData.length;
            int copyLength = i + recvLength > length ? length - i : recvLength;
            System.arraycopy(recvData, 0, data, i, copyLength);
            i += recvData.length;
        }
        return data;
    }

    public void set(String key, String value) throws IOException {
        set(key, 0, value);
    }

    public void set(String key, int expiration, String value) throws IOException {
        byte[] valueData = value.getBytes(CHARSET_UTF8);
        String command = String.format("set %s 0 %d %d", key, expiration, valueData.length);
        send(command);
        send(valueData);

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
        String command = String.format("get %s", key);
        return rawGet(command);
    }

    public String get(String key, long timeout) throws IOException {
        String command = String.format("get %s/t=%d", key, timeout);
        return rawGet(command);
    }

    public String peek(String key) throws IOException {
        String command = String.format("get %s/peek", key);
        return rawGet(command);
    }

    public String peek(String key, long timeout) throws IOException {
        String command = String.format("get %s/peek/t=%d", key, timeout);
        return rawGet(command);
    }
}
