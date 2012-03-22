/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;

/**
 * Miscellaneous utility functions.
 * 
 * @author Steven R. Emmerson
 */
public final class Misc {
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
        System.out.print("Executing: ");
        final List<String> args = builder.command();
        for (final String arg : args) {
            System.out.print(arg);
            System.out.print(' ');
        }
        System.out.println();
        final Process process = builder.start();
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        return status;
    }
}
