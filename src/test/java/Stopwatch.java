/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */

/**
 * A stopwatch.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Stopwatch {
    private long startTime;
    private long elapsedTime;

    /**
     * Starts accumulating time.
     */
    void start() {
        startTime = System.nanoTime();
    }

    /**
     * Stops accumulating time.
     */
    void stop() {
        elapsedTime += System.nanoTime() - startTime;
    }

    /**
     * Resets the accumulated time to zero.
     */
    void reset() {
        elapsedTime = 0;
    }

    /**
     * Returns the accumulated time.
     * 
     * @return The accumulated time in seconds
     */
    double getAccumulatedTime() {
        return elapsedTime / 1e9;
    }
}
