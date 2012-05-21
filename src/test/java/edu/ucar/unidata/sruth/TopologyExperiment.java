package edu.ucar.unidata.sruth;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * 
 * 
 * Instances are .
 * 
 * @author Steven R. Emmerson
 */
final class TopologyExperiment {
    public static void main(final String[] args) throws IOException,
            ClassNotFoundException {
        final File file = new File(
                "/tmp/PubSubTest/subscribers/1/SRUTH/gilda.unidata.ucar.edu:38800/Topology");
        final InputStream inputStream = new FileInputStream(file);
        final ObjectInputStream ois = new ObjectInputStream(inputStream);
        ois.readObject();
        ois.close();
    }
}
