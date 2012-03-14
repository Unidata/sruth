/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;


/**
 * Listener for data-products.
 * 
 * @author Steven R. Emmerson
 */
interface DataProductListener {
    /**
     * Process a data-product.
     * 
     * @param dataProduct
     *            The data-product to be processed.
     */
    public void process(DataProduct dataProduct);
}
