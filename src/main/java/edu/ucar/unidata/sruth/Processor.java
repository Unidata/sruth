/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.jcip.annotations.ThreadSafe;

/**
 * Processes data-products according to client instructions.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
public final class Processor {
    /**
     * Map from filters to actions.
     */
    private final ConcurrentMap<Pattern, List<Action>> actions = new ConcurrentHashMap<Pattern, List<Action>>();

    /**
     * Adds a processing action to a data-product category.
     * 
     * @param pattern
     *            The pattern that selects the relevant data-products.
     * @param action
     *            The action to be executed on data-products that pass the
     *            filter.
     */
    public void add(final Pattern pattern, final Action action) {
        final List<Action> newList = new LinkedList<Action>();
        List<Action> list = actions.putIfAbsent(pattern, newList);
        if (list == null) {
            list = newList;
        }
        synchronized (list) {
            list.add(action);
        }
    }

    /**
     * Processes a data-product. A data-product will be acted upon by matching
     * actions in the order in which the actions were added.
     * 
     * @param dataProduct
     *            The data-product to process.
     * @return <code>true</code> if and only if the given data-product was
     *         selected for processing.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean process(final DataProduct dataProduct) throws IOException,
            InterruptedException {
        boolean processed = false;
        for (final Map.Entry<Pattern, List<Action>> entry : actions.entrySet()) {
            final Matcher matcher = dataProduct.matcher(entry.getKey());
            if (matcher.matches()) {
                for (final Action action : entry.getValue()) {
                    // Foreign method. Don't call while synchronized
                    action.execute(matcher, dataProduct);
                    processed = true;
                }
            }
        }
        return processed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + " [" + actions.size() + " actions]";
    }
}
