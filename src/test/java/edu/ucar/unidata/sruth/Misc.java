/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.util.List;
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
     * @param cmd
     *            The command to execute
     * @return The status of the executed command. 0 means success.
     * @throws IOException
     * @throws InterruptedException
     */
    public static int system(final String... cmd) throws IOException,
            InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.inheritIO();
        final StringBuilder msg = new StringBuilder("Executing: ");
        final List<String> args = builder.command();
        for (final String arg : args) {
            msg.append(arg);
            msg.append(' ');
        }
        logger.info(msg.toString());
        final Process process = builder.start();
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        return status;
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
