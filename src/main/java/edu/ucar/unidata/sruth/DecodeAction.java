/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.jcip.annotations.NotThreadSafe;

import org.slf4j.Logger;

/**
 * Pipes a data-product to a command.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
public final class DecodeAction extends Action {
    /**
     * The logger for this class.
     */
    private static Logger  logger = Util.getLogger();
    /**
     * The decoder command.
     */
    private final String[] command;

    /**
     * Constructs from the decoder command.
     * 
     * @param command
     *            The individual arguments of the decoder command. Each argument
     *            may contain references to the subsequences of capturing groups
     *            matched by {@link DataProduct#matcher(Pattern)} of the form
     *            "$i", where "i" is the i-th capturing group.
     * @throws NullPointerException
     *             if {@code command == null}.
     */
    public DecodeAction(final String[] command) {
        this.command = command.clone();
    }

    /**
     * Constructs from the decoder command.
     * 
     * @param command
     *            The individual arguments of the decoder command. Each argument
     *            may contain references to the subsequences of capturing groups
     *            matched by {@link DataProduct#matcher(Pattern)} of the form
     *            "$i", where "i" is the i-th capturing group.
     * @throws NullPointerException
     *             if {@code command == null}.
     */
    public DecodeAction(final List<String> command) {
        this.command = command.toArray(new String[command.size()]);
    }

    @Override
    protected void execute(final Matcher matcher, final DataProduct dataProduct)
            throws IOException, InterruptedException {
        assert dataProduct.matches(matcher.pattern());
        final Replacer replacer = getReplacer(matcher);
        final String[] cmd = command.clone();
        for (int argIndex = 0; argIndex < cmd.length; argIndex++) {
            cmd[argIndex] = replacer.replace(cmd[argIndex]);
        }
        decode(dataProduct, cmd);
    }

    /**
     * Decodes a data-product. A decoder subprocess is started on whose standard
     * input stream the data-product appears, whose error stream is logged by
     * this method, and whose output stream is inherited from this process. This
     * method waits for the decoder subprocess to terminate.
     * 
     * @param decoder
     *            The individual arguments of the decoder command.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     * @see ProcessBuilder#ProcessBuilder(String[])
     */
    private void decode(final DataProduct dataProduct, final String... decoder)
            throws IOException, InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder(decoder);
        /*
         * Redirect the decoder's input stream to the data-product file,
         * redirect the decoder's output stream to this process' output stream,
         * and redirect the decoder's error stream back to this process.
         */
        builder.redirectInput(dataProduct.getAbsolutePath());
        builder.redirectOutput(Redirect.INHERIT);
        final Process process = builder.start();
        try {
            /*
             * Log the decoder's error stream.
             */
            final BufferedReader errorStream = new BufferedReader(
                    new InputStreamReader(process.getErrorStream()));
            try {
                for (String line = errorStream.readLine(); line != null; line = errorStream
                        .readLine()) {
                    logger.error(line);
                }

                // The process should have terminated
                process.waitFor();
                final int status = process.exitValue();
                if (status != 0) {
                    logger.error("Decoder \"{}\" terminated with status {}",
                            Util.getCommand(builder.command()),
                            Integer.valueOf(status));
                }
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

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.Action#toString()
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + " [command=\""
                + Util.formatCommand(command) + "\"]";
    }
}
