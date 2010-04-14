/**
 * 
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;

/**
 * Requests data from remote server.
 * 
 * @author Steven R. Emmerson
 */
final class Request implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
