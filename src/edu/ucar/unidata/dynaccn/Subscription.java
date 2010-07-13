package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */

/**
 * A data subscription.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Subscription {
    /**
     * The address of the tracker.
     */
    private final SocketAddress trackerAddress;
    /**
     * The data-selection predicate.
     */
    private final Predicate     predicate;

    /**
     * Constructs from a tracker's address and a data-selection predicate.
     * 
     * @param trackerAddress
     *            The tracker's address.
     * @param predicate
     *            The data-selection predicate.
     * @throws NullPointerException
     *             if {@code trackerAddress == null || predicate == null}.
     */
    Subscription(final SocketAddress trackerAddress, final Predicate predicate) {
        if (null == trackerAddress) {
            throw new NullPointerException();
        }
        if (null == predicate) {
            throw new NullPointerException();
        }
        this.trackerAddress = trackerAddress;
        this.predicate = predicate;
    }

    /**
     * Constructs from an XML file.
     * 
     * @param path
     *            Pathname of the XML file.
     * @throws IOException
     *             if an I/O error occurs
     * @throws SAXException
     *             if an XML parse error occurs.
     * @throws ParserConfigurationException
     *             if a document-builder can't be created.
     * @throws IllegalArgumentException
     *             if the XML file is invalid.
     */
    Subscription(final Path path) throws SAXException, IOException,
            ParserConfigurationException {
        /*
         * Get document.
         */
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
                .newInstance();
        final DocumentBuilder docBuilder = docBuilderFactory
                .newDocumentBuilder();
        final Document doc = docBuilder.parse(new File(path.toString()));
        final Element docElt = doc.getDocumentElement();
        /*
         * Extract tracker information.
         */
        NodeList nodeList = docElt.getElementsByTagName("tracker");
        if (nodeList.getLength() != 1) {
            throw new IllegalArgumentException("nodeList.getLength() = "
                    + nodeList.getLength());
        }
        Element elt = (Element) nodeList.item(0);
        String attrString = elt.getAttribute("host");
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(attrString);
        }
        catch (final UnknownHostException e) {
            throw new IllegalArgumentException("Unknown host: \"" + attrString
                    + "\"");
        }
        attrString = elt.getAttribute("port");
        int port;
        try {
            port = Integer.valueOf(attrString);
        }
        catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: \"" + attrString
                    + "\"");
        }
        trackerAddress = new InetSocketAddress(inetAddress, port);
        /*
         * Extract selection predicate.
         */
        nodeList = docElt.getElementsByTagName("predicate");
        if (nodeList.getLength() != 1) {
            throw new IllegalArgumentException("nodeList.getLength() = "
                    + nodeList.getLength());
        }
        elt = (Element) nodeList.item(0);
        final String type = elt.getAttribute("type");
        if (type.equalsIgnoreCase("everything")) {
            predicate = Predicate.EVERYTHING;
        }
        else if (type.equalsIgnoreCase("nothing")) {
            predicate = Predicate.NOTHING;
        }
        else {
            // TODO: add filters
            throw new IllegalArgumentException("Unknown predicate: \"" + type
                    + "\"");
        }
    }

    /**
     * @return the trackerAddress
     */
    SocketAddress getTrackerAddress() {
        return trackerAddress;
    }

    /**
     * @return the predicate
     */
    Predicate getPredicate() {
        return predicate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Subscription [predicate=" + predicate + ", trackerAddress="
                + trackerAddress + "]";
    }
}
