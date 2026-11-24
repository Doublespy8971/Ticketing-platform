package com.ticketing.e2e.support;

import java.time.Duration;
import java.util.List;

/**
 * Abstraction for executing system processes with explicit timeouts.
 */
public interface ProcessRunner {

    CommandResult exec(Duration timeout, List<String> command);

    default CommandResult exec(Duration timeout, String... command) {
        return exec(timeout, List.of(command));
    }
}
