package org.hogel.kestrel;



public class KestrelCommandFactory {
    private static final String CRLF = "\r\n";

    public KestrelCommandFactory() {
    }

    private String createCommand(String... values) {
        int bufsize = 0;
        for (String value : values) {
            bufsize += value.length();
        }
        char[] cmdbuf = new char[bufsize];
        int index = 0;
        for (String value : values) {
            index = copychars(cmdbuf, index, value);
        }
        return new String(cmdbuf);
    }

    private int copychars(char[] buf, int index, String append) {
        int length = append.length();
        append.getChars(0, length, buf, index);
        return index + length;
    }

    private static final String SET_SP = "set ";
    private static final String SP_ZERO_SP_ZERO_SP = " 0 0 ";

    public String setCommand(String key, byte[] data) {
        return createCommand(SET_SP, key, SP_ZERO_SP_ZERO_SP, Long.toString(data.length), CRLF);
    }

    private static final String SP_ZERO_SP = " 0 ";
    private static final String SP = " ";

    public String setCommand(String key, long expiration, byte[] data) {
        return createCommand(SET_SP, key, SP_ZERO_SP, Long.toString(expiration), SP, Integer.toString(data.length), CRLF);
    }

    private static final String GET_SP = "get ";

    public String getCommand(String key) {
        return createCommand(GET_SP, key, CRLF);
    }

    private static final String SLASH_T_EQ = "/t=";

    public String getCommand(String key, long timeout) {
        return createCommand(GET_SP, key, SLASH_T_EQ, Long.toString(timeout), CRLF);
    }

    private static final String SLASH_PEEK = "/peek";

    public String peekCommand(String key) {
        return createCommand(GET_SP, key, SLASH_PEEK, CRLF);
    }

    public String peekCommand(String key, long timeout) {
        return createCommand(GET_SP, key, SLASH_PEEK, SLASH_T_EQ, Long.toString(timeout), CRLF);
    }

    private static final String DELETE_SP = "delete ";

    public String deleteCommand(String key) {
        return createCommand(DELETE_SP, key, CRLF);
    }
}
