/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.slf4j.Logger;

/**
 * Miscellaneous utility functions.
 * 
 * @author Steven R. Emmerson
 */
public final class Misc {
    private static final Logger logger = Util.getLogger();

    /**
     * Executes a system command.
     * 
     * @param args
     *            The arguments of the command to execute
     * @return The status of the executed command. 0 means success.
     * @throws IOException
     * @throws InterruptedException
     */
    public static int system(final String... args) throws IOException,
            InterruptedException {
        return system(false, args);
    }

    /**
     * Executes a system command.
     * 
     * @param mergeOutput
     *            Whether or not to merge the output stream and the error stream
     *            of the child process. Useful for utilities like diff(1), which
     *            print "Only in ..." messages to the output stream.
     * @param args
     *            The arguments of the command to execute
     * @return The status of the executed command. 0 means success.
     * @throws IOException
     * @throws InterruptedException
     */
    public static int system(final boolean mergeOutput, final String... args)
            throws IOException, InterruptedException {
        final StringBuilder msg = new StringBuilder();
        for (final String arg : args) {
            msg.append(arg);
            msg.append(' ');
        }
        logger.info("Executing: {}", msg.toString());
        final ProcessBuilder builder = new ProcessBuilder(args);
        builder.redirectInput(Redirect.INHERIT);
        if (mergeOutput) {
            builder.redirectErrorStream(true);
        }
        else {
            builder.redirectOutput(Redirect.INHERIT);
        }
        final Process process = builder.start();
        Assert.assertNotNull(process);
        try {
            /*
             * Log the child process' error stream.
             */
            final BufferedReader errorStream = new BufferedReader(
                    new InputStreamReader(mergeOutput
                            ? process.getInputStream()
                            : process.getErrorStream()));
            try {
                for (String line = errorStream.readLine(); line != null; line = errorStream
                        .readLine()) {
                    logger.error(line);
                }

                // The process should have terminated
                return process.waitFor();
            }
            finally {
                try {
                    errorStream.close();
                }
                catch (final IOException ignored) {
                }
            }
        }
        finally {
            process.destroy();
        }
    }

    /**
     * Returns a reporting task.
     * 
     * @param completionService
     *            The {@link CompletionService} on whose tasks to report
     * @return a reporting task
     */
    public static <T> Callable<T> newReportingTask(
            final CompletionService<T> completionService) {
        return new Callable<T>() {
            @Override
            public T call() throws InterruptedException {
                for (;;) {
                    final Future<T> future = completionService.take();
                    if (!future.isCancelled()) {
                        try {
                            future.get();
                        }
                        catch (final ExecutionException e) {
                            final Throwable cause = e.getCause();
                            if (!(cause instanceof InterruptedException)) {
                                logger.error("Unexpected error", cause);
                            }
                        }
                    }
                }
            }
        };
    }

    /**
     * Returns a reporting task.
     * 
     * @param completionService
     *            The {@link CompletionService} on whose tasks to report
     * @return a reporting task
     */
    public static <T> Callable<T> newReportingTask(
            final CompletionService<T> completionService) {
        return new Callable<T>() {
            @Override
            public T call() throws InterruptedException {
                for (;;) {
                    final Future<T> future = completionService.take();
                    if (!future.isCancelled()) {
                        try {
                            future.get();
                        }
                        catch (final ExecutionException e) {
                            final Throwable cause = e.getCause();
                            if (!(cause instanceof InterruptedException)) {
                                logger.error("Unexpected error", cause);
                            }
                        }
                    }
                }
            }
        };
    }
}
