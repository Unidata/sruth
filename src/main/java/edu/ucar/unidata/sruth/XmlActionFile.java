/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import net.jcip.annotations.ThreadSafe;

import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 * Factory methods for generating a {@link Processor} from XML.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class XmlActionFile {
    /**
     * Returns the {@link Processor} corresponding to XML input.
     * 
     * @param input
     *            The XML input.
     * @return The {@link Processor} corresponding to the XML input.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code input == null}.
     */
    static Processor getProcessor(final InputStream input) throws IOException {
        Processor processor;
        final SAXBuilder builder = new SAXBuilder();
        try {
            final Document doc = builder.build(input);
            processor = process(doc);
        }
        catch (final JDOMException e) {
            throw (IOException) new IOException().initCause(e);
        }
        return processor;
    }

    /**
     * Returns the {@link Processor} corresponding to an XML file.
     * 
     * @param url
     *            The URL of the XML file.
     * @return The {@link Processor} corresponding to the XML file.
     * @throws FileNotFoundException
     *             if the specified file doesn't exist.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    static Processor getProcessor(final URL url) throws IOException {
        Processor processor;
        final SAXBuilder builder = new SAXBuilder();
        try {
            final Document doc = builder.build(url);
            processor = process(doc);
        }
        catch (final JDOMException e) {
            throw (IOException) new IOException().initCause(e);
        }
        return processor;
    }

    /**
     * Returns the {@link Processor} corresponding to an XML string.
     * 
     * @param xml
     *            The XML string.
     * @return The {@link Processor} corresponding to the XML string.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code xml == null}.
     */
    static Processor getProcessor(final String xml) throws IOException {
        Processor processor;
        final SAXBuilder builder = new SAXBuilder();
        try {
            final Document doc = builder.build(new StringReader(xml));
            processor = process(doc);
        }
        catch (final JDOMException e) {
            throw (IOException) new IOException().initCause(e);
        }
        return processor;
    }

    /**
     * Converts a JDOM document into a processor of data-products.
     * 
     * @param doc
     *            The JDOM document to convert.
     * @return The corresponding processor of data-products.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static Processor process(final Document doc) throws IOException {
        final Processor processor = new Processor();
        final Element rootElt = doc.getRootElement();
        processRootElt(rootElt, processor);
        return processor;
    }

    /**
     * Processes the root element.
     * 
     * @param elt
     *            The root element.
     * @param processor
     *            The processor of data-products to be set.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @SuppressWarnings("unchecked")
    private static void processRootElt(final Element rootElt,
            final Processor processor) throws IOException {
        if (!"actions".equalsIgnoreCase(rootElt.getName())) {
            throw new IOException("No \"actions\" element");
        }
        for (final Element elt : (List<Element>) rootElt.getChildren("entry")) {
            processEntryElt(elt, processor);
        }
    }

    /**
     * Returns a required attribute.
     * 
     * @param elt
     *            The element whose attribute is to be returned.
     * @param name
     *            The name of the attribute.
     * @return The value of the attribute.
     * @throws IOException
     *             if the given attribute doesn't exist.
     */
    private static String getAttribute(final Element elt, final String name)
            throws IOException {
        final Attribute attr = elt.getAttribute(name);
        if (attr == null) {
            throw new IOException("Missing attribute: + \"" + name + "\"");
        }
        return attr.getValue();
    }

    /**
     * Processes an "entry" element.
     * 
     * @param elt
     *            The "entry" element.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @SuppressWarnings("unchecked")
    private static void processEntryElt(final Element entryElt,
            final Processor processor) throws IOException {
        final String string = getAttribute(entryElt, "pattern");
        final Pattern pattern = Pattern.compile(string);
        for (final Element elt : (List<Element>) entryElt.getChildren()) {
            final String eltName = elt.getName();
            if (eltName.equalsIgnoreCase("file")) {
                processFileElt(elt, pattern, processor);
            }
            else if (eltName.equalsIgnoreCase("decode")) {
                processDecodeElt(elt, pattern, processor);
            }
        }
    }

    /**
     * Processes a "file" element.
     * 
     * @param fileElt
     *            The "file" element.
     * @param pattern
     *            The pattern that selects data-products to be filed.
     * @param processor
     *            The processor of data-products.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static void processFileElt(final Element fileElt,
            final Pattern pattern, final Processor processor)
            throws IOException {
        final String path = getAttribute(fileElt, "path");
        final Action action = new FileAction(path);
        processor.add(pattern, action);
    }

    /**
     * Processes a "decode" element.
     * 
     * @param decodeElt
     *            The "decode" element.
     * @param pattern
     *            The pattern that selects data-products to be decoded.
     * @param processor
     *            The processor of data-products.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @SuppressWarnings("unchecked")
    private static void processDecodeElt(final Element decodeElt,
            final Pattern pattern, final Processor processor)
            throws IOException {
        final List<String> command = new LinkedList<String>();
        String string = getAttribute(decodeElt, "program");
        command.add(string);
        for (final Element elt : (List<Element>) decodeElt.getChildren()) {
            final String eltName = elt.getName();
            if (eltName.equalsIgnoreCase("arg")) {
                string = elt.getTextTrim();
                command.add(string);
            }
        }
        final Action action = new DecodeAction(command);
        processor.add(pattern, action);
    }
}
