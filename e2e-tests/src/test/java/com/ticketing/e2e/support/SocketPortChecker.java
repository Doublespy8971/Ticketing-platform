package com.ticketing.e2e.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

/**
 * Standard {@link PortChecker} implementation creating a socket connection.
 */
public class SocketPortChecker implements PortChecker {

    @Override
    public boolean isPortOpen(String host, int port, Duration timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
