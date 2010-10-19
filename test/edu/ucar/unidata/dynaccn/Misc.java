/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;

import org.junit.Assert;

/**
 * Miscellaneous utility functions.
 * 
 * @author Steven R. Emmerson
 */
final class Misc {
    /**
     * Executes a system command.
     * 
     * @param cmd
     *            The command to execute
     * @return The status of the executed command. 0 means success.
     * @throws IOException
     * @throws InterruptedException
     */
    static int system(final String... cmd) throws IOException,
            InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.inheritIO();
        final Process process = builder.start();
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        return status;
    }
}
