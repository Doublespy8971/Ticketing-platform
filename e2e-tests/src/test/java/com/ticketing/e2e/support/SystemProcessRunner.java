package com.ticketing.e2e.support;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default {@link ProcessRunner} using Java's {@link ProcessBuilder}.
 * Reads stdout and stderr concurrently to prevent OS pipe buffer deadlocks on large outputs.
 */
public class SystemProcessRunner implements ProcessRunner {

    @Override
    public CommandResult exec(Duration timeout, List<String> command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);
        try {
            Process proc = pb.start();
            CompletableFuture<byte[]> stdoutFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return proc.getInputStream().readAllBytes();
                } catch (IOException e) {
                    return new byte[0];
                }
            });
            CompletableFuture<byte[]> stderrFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return proc.getErrorStream().readAllBytes();
                } catch (IOException e) {
                    return new byte[0];
                }
            });

            boolean finished = proc.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                proc.destroyForcibly();
                stdoutFuture.cancel(true);
                stderrFuture.cancel(true);
                throw new IllegalStateException("Command timed out after " + timeout.toSeconds()
                        + "s: " + String.join(" ", command));
            }
            byte[] stdoutBytes = stdoutFuture.join();
            byte[] stderrBytes = stderrFuture.join();
            return new CommandResult(
                    proc.exitValue(),
                    new String(stdoutBytes, StandardCharsets.UTF_8),
                    new String(stderrBytes, StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            return new CommandResult(127, "", e.getMessage() != null ? e.getMessage() : e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while running command: " + String.join(" ", command), e);
        }
    }
}
