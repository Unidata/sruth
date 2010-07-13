package edu.ucar.unidata.dynaccn;

import java.util.concurrent.Callable;

/**
 * A task that can be explicitly stopped. This can be necessary if, for example,
 * a task calls non-interruptible blocking methods (e.g., socket I/O) and yet
 * it's inconvenient to use {@link BlockingTask}.
 * 
 * @author Steven R. Emmerson
 */
interface StoppableTask<T> extends Callable<T> {
    /**
     * Stops the task.
     */
    void stop();
}