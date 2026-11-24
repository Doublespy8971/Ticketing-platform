package com.ticketing.e2e.support;

import java.time.Duration;

/**
 * Strategy interface to probe whether a local or remote TCP port is accepting connections.
 */
public interface PortChecker {

    boolean isPortOpen(String host, int port, Duration timeout);
}
