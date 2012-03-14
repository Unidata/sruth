/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

/**
 * A processing action to be applied to data-products.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
public abstract class Action {
    /**
     * Replaces references to subsequences of capturing groups.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    static final class Replacer {
        private final Matcher matcher;

        /**
         * Constructs from the {@link Matcher} returned by
         * {@link DataProduct#matcher(java.util.regex.Pattern)}.
         * 
         * @param matcher
         *            The {@link Matcher} returned by
         *            {@link DataProduct#matcher(java.util.regex.Pattern)}.
         */
        Replacer(final Matcher matcher) {
            this.matcher = matcher;
        }

        /**
         * Replaces all references to matched subsequences.
         * 
         * @param string
         *            The string to have its references replaced.
         * @return The given string with all references replaced.
         */
        String replace(final String string) {
            String str = string;
            final int groupCount = matcher.groupCount();
            for (int groupIndex = 1; groupIndex <= groupCount; groupIndex++) {
                final Pattern capturingGroupPat = Pattern
                        .compile("([^\\\\]?)\\$" + groupIndex + "\\b");
                final String replacement = "$1"
                        + Matcher.quoteReplacement(matcher.group(groupIndex));
                str = capturingGroupPat.matcher(str).replaceAll(replacement);
            }
            return str;
        }
    }

    /**
     * Performs the action on a data-product.
     * 
     * @param matcher
     *            The {@link Matcher} returned from
     *            {@link DataProduct#matcher(java.util.regex.Pattern)}.
     * @param dataProduct
     *            The data-product to be acted upon.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    abstract void execute(Matcher matcher, final DataProduct dataProduct)
            throws IOException, InterruptedException;

    /**
     * Returns a replacer of references to the subsequences of capturing groups.
     * 
     * @param matcher
     *            The result of
     *            {@link DataProduct#matcher(java.util.regex.Pattern)}.
     * @return a replacer of references to the subsequences of capture groups of
     *         the given {@link Matcher}.
     */
    protected static Replacer getReplacer(final Matcher matcher) {
        return new Replacer(matcher);
    }

    @Override
    public abstract String toString();
}
