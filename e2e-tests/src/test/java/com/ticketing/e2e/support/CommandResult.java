package com.ticketing.e2e.support;

/**
 * Result of executing a system command via a {@link ProcessRunner}.
 */
public record CommandResult(int exitCode, String stdout, String stderr) {
}
