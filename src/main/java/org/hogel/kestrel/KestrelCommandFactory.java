package org.hogel.kestrel;

import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





public class KestrelCommandFactory {
    static final Logger LOG = LoggerFactory.getLogger(KestrelCommandFactory.class);

    public static class CommandCache extends TreeMap<Object, CommandCache> {
        private static final long serialVersionUID = 1L;
        final String command;
        public CommandCache(String command) {
            this.command = command;
        }
        public String getCommand() {
            return command;
        }
        synchronized public String getCachedCommand(Object... values) {
            CommandCache cache = this;
            String command = cache.getCommand();
            for (Object value : values) {
                CommandCache next = cache.get(value);
                if (next == null) {
                    next = new CommandCache(command + value.toString());
                    cache.put(value, next);
                }
                command = next.getCommand();
                cache = next;
            }
            return cache.getCommand();
        }
    }

    public KestrelCommandFactory() {
    }

    private String createCommand(CommandCache commandCache, Object... values) {
        return commandCache.getCachedCommand(values);
    }

    private static final String CRLF = "\r\n";

    private final CommandCache setCommandCache = new CommandCache("set ");
    public String setCommand(String key, byte[] data) {
        return createCommand(setCommandCache, key, " 0 0 ", data.length, CRLF);
    }

    private final CommandCache setExpCommandCache = new CommandCache("set ");
    public String setCommand(String key, long expiration, byte[] data) {
        return createCommand(setExpCommandCache, key, " 0 ", expiration, " ", data.length, CRLF);
    }

    private final CommandCache getCommandCache = new CommandCache("get ");
    public String getCommand(String key) {
        return createCommand(getCommandCache, key, CRLF);
    }

    private final CommandCache getTimeoutCommandCache = new CommandCache("get ");
    public String getCommand(String key, long timeout) {
        return createCommand(getTimeoutCommandCache, key, "/t=", timeout, CRLF);
    }

    private final CommandCache peekCommandCache = new CommandCache("get ");
    public String peekCommand(String key) {
        return createCommand(peekCommandCache, key, "/peek", CRLF);
    }

    private final CommandCache peekTimeoutCommandCache = new CommandCache("get ");
    public String peekCommand(String key, long timeout) {
        return createCommand(peekTimeoutCommandCache, key, "/peek/t=", timeout, CRLF);
    }

    private final CommandCache deleteCommandCache = new CommandCache("delete ");
    public String deleteCommand(String key) {
        return createCommand(deleteCommandCache, key, CRLF);
    }
}
