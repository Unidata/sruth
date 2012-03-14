/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.awt.Container;
import java.awt.Point;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.WindowConstants;
import javax.xml.parsers.ParserConfigurationException;

import net.miginfocom.layout.CC;
import net.miginfocom.swing.MigLayout;

import org.slf4j.Logger;
import org.xml.sax.SAXException;

/**
 * Graphical user interface to a {@link Subscriber}.
 * 
 * @author Steven R. Emmerson
 */
public final class SubscriberApp {
    /**
     * The display update task.
     * 
     * @author Steven R. Emmerson
     */
    private final class UpdateTask extends SwingWorker<Void, Void> {
        @Override
        protected Void doInBackground() {
            long fileCount = 0;
            int peerCount = 0;
            while (!isCancelled()) {
                try {
                    Thread.sleep(1000);
                }
                catch (final InterruptedException e) {
                    break; // treat as termination request
                }
                final long oldCount = fileCount;
                fileCount = subscriber.getReceivedFileCount();
                firePropertyChange(RECEIVED_FILE_COUNT, oldCount, fileCount);

                final int oldPeerCount = peerCount;
                peerCount = subscriber.getPeerCount();
                firePropertyChange(PEER_COUNT, oldPeerCount, peerCount);
            }
            return null;
        }
    }

    /**
     * The {@link Subscriber} execution task.
     * 
     * If this class subclasses {@link SwingWorker}, then the {@link Subscriber}
     * object won't be executed for some reason (perhaps because this object
     * stays in the {@link SwingWorkder} queue).
     * 
     * @author Steven R. Emmerson
     */
    private final class SubscriberTask extends FutureTask<Void> {
        SubscriberTask() {
            super(subscriber);
        }

        @Override
        public void run() {
            try {
                subscriber.call();
            }
            catch (Throwable e) {
                if (!(e instanceof InterruptedException)) {
                    if (e instanceof ExecutionException) {
                        e = e.getCause();
                    }
                    logger.error("Fatal error", e);
                    JOptionPane.showMessageDialog(null, "I'm sorry, but I "
                            + "encountered the following fatal error:\n" + e,
                            getClass().getSimpleName(),
                            JOptionPane.ERROR_MESSAGE);
                    System.exit(2);
                }
            }
        }
    }

    /**
     * Serial version ID.
     */
    private static final long        serialVersionUID       = 1L;
    /**
     * The logger for this class.
     */
    private static final Logger      logger                 = Util.getLogger();
    /**
     * The user preferences.
     */
    private static final Preferences prefs                  = Preferences
                                                                    .userNodeForPackage(SubscriberApp.class);
    /**
     * The pathname of the root of the application installation.
     */
    private static final String      INSTALL_ROOT           = "installationRoot";
    /**
     * The pathname of the root directory preference.
     */
    private static final String      ROOT_DIR               = "archiveDir";
    /**
     * The pathname of the actions-file path preference.
     */
    private static final String      ACTIONS_FILE_PATH      = "actionsFile";
    /**
     * The name of the x-position preference.
     */
    private static final String      X_POSITION             = "xPosition";
    /**
     * The name of the y-position preference.
     */
    private static final String      Y_POSITION             = "yPosition";
    /**
     * The name of the preference that's the number of received files.
     */
    private static final String      RECEIVED_FILE_COUNT    = "receivedFileCount";
    /**
     * The name of the preference that's the number of peers.
     */
    private static final String      PEER_COUNT             = "peerCount";
    /**
     * The name of the preference that's the minimum potential port number for
     * the server.
     */
    private static final String      MIN_PORT               = "minPort";
    /**
     * The name of the preference that's the maximum potential port number for
     * the server.
     */
    private static final String      MAX_PORT               = "maxPort";
    /**
     * The pathname of the root directory of the archive.
     */
    private File                     archiveDir;
    /**
     * The pathname of the file that contains local processing actions.
     */
    private File                     actionsFile;
    /**
     * The subscription.
     */
    private final Subscription       subscription;
    /**
     * The top panel.
     */
    private final JPanel             topPanel               = new JPanel();
    /**
     * The bottom panel.
     */
    private final JPanel             botPanel               = new JPanel();
    /**
     * The listener for when done.
     */
    private final ActionListener     doneListener;
    /**
     * The container into which to put widgets.
     */
    private final Container          container;
    /**
     * The label containing the pathname of the archive.
     */
    private final JLabel             archiveDirField;
    /**
     * The label containing the pathname of the actions-file.
     */
    private final JLabel             actionsFileField;
    /**
     * The button for changing the pathname of the archive.
     */
    private final JButton            archiveChangeButton;
    /**
     * The button for changing the pathname of the processing-actions file.
     */
    private final JButton            actionsChangeButton;
    /**
     * The button for executing the program.
     */
    private final JButton            executeButton;
    /**
     * The number of received files.
     */
    private final JLabel             receivedFileCountField = new JLabel("0");
    /**
     * The number of connected peers.
     */
    private final JLabel             peerCountField         = new JLabel("0");
    /**
     * The updating task.
     */
    private UpdateTask               updateTask;
    /**
     * The subscriber.
     */
    private Subscriber               subscriber;
    /**
     * The thread on which the subscriber executes.
     */
    private SubscriberTask           subscriberTask;
    /**
     * The minimum potential port number for the server.
     */
    private int                      minPort;
    /**
     * The maximum potential port number for the server.
     */
    private int                      maxPort;
    /**
     * The text field for the minimum potential port number for the server.
     */
    private final JTextField         minPortField;
    /**
     * The text field for the maximum potential port number for the server.
     */
    private final JTextField         maxPortField;

    /**
     * Constructs from the pathname of a subscription file.
     * 
     * @param path
     *            The pathname of the subscription file.
     * @param container
     *            The AWT container in which to put widgets.
     * @param doneListener
     *            The listener to call when this instance is done.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code path == null || container == null || doneListener
     *             == null}.
     * @throws ParserConfigurationException
     *             if a parser for the subscription file can't be created.
     * @throws SAXException
     *             if the subscription file can't be parsed.
     */
    SubscriberApp(final Path path, final Container container,
            final ActionListener doneListener) throws SAXException,
            IOException, ParserConfigurationException {
        if (null == path || null == container || null == doneListener) {
            throw new NullPointerException();
        }
        this.container = container;
        this.doneListener = doneListener;
        subscription = new Subscription(Files.newInputStream(path));
        {
            /*
             * Top panel:
             */
            topPanel.setLayout(new MigLayout("wrap 3"));
            {
                /*
                 * Tracker:
                 */
                final JLabel trackerLbl = new JLabel("Tracker:");
                topPanel.add(trackerLbl, "align right");
                final JLabel trackerField = new JLabel(subscription
                        .getTrackerAddress().toString());
                topPanel.add(trackerField, "span");
            }
            {
                /*
                 * Predicate:
                 */
                final JLabel predicateLbl = new JLabel("Predicate:");
                topPanel.add(predicateLbl, "align right");
                final JLabel predicateField = new JLabel(subscription
                        .getPredicate().toString());
                topPanel.add(predicateField, "span");
            }
            // Root directory of this application:
            final String appDir = prefs.get(INSTALL_ROOT,
                    System.getProperty("user.home") + File.separator
                            + this.getClass().getSimpleName());
            {
                /*
                 * Root directory of the archive:
                 */
                final String archiveDirPref = prefs.get(ROOT_DIR, appDir
                        + File.separator + "archive");
                archiveDir = new File(archiveDirPref);
                final JLabel archiveDirLbl = new JLabel("Archive:");
                topPanel.add(archiveDirLbl, "align right");
                archiveDirField = new JLabel(archiveDir.toString());
                topPanel.add(archiveDirField);
                archiveChangeButton = new JButton("Change...");
                archiveChangeButton.setMnemonic('h');
                archiveChangeButton.setToolTipText("Choose archive directory");
                archiveChangeButton.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(final ActionEvent e) {
                        chooseFile("Choose Archive Directory",
                                "Use selected directory for data archive",
                                archiveDirField, JFileChooser.DIRECTORIES_ONLY);
                    }
                });
                topPanel.add(archiveChangeButton);
            }
            {
                /*
                 * Processing configuration file:
                 */
                final String actionsFilePathPref = prefs.get(ACTIONS_FILE_PATH,
                        appDir + File.separator + "processing.xml");
                actionsFile = new File(actionsFilePathPref);
                final JLabel actionsFileLbl = new JLabel(
                        "Processing configuration file:");
                topPanel.add(actionsFileLbl, "align right");
                actionsFileField = new JLabel(actionsFile.toString());
                topPanel.add(actionsFileField);
                actionsChangeButton = new JButton("Change...");
                actionsChangeButton.setMnemonic('a');
                actionsChangeButton
                        .setToolTipText("Choose processing-actions file");
                actionsChangeButton.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(final ActionEvent e) {
                        chooseFile("Choose Processing-Actions File",
                                "Use selected file for processing actions",
                                actionsFileField,
                                JFileChooser.FILES_AND_DIRECTORIES);
                    }
                });
                topPanel.add(actionsChangeButton);
            }
            {
                /*
                 * Port range:
                 */
                {
                    /*
                     * Minimum port number:
                     */
                    minPort = prefs.getInt(MIN_PORT, 1);
                    final JLabel minPortLbl = new JLabel("Min:");
                    minPortField = new JTextField(Integer.toString(
                            PortNumberSet.MAX_PORT_NUMBER).length());
                    minPortField.setText(Integer.toString(minPort));
                    minPortField.setToolTipText("Minimum potential port (1 - "
                            + PortNumberSet.MAX_PORT_NUMBER + ")");
                    final JLabel portRangeLbl = new JLabel("Server Port Range:");
                    topPanel.add(portRangeLbl);
                    final JPanel minPortPanel = new JPanel();
                    new BoxLayout(minPortPanel, BoxLayout.X_AXIS);
                    minPortPanel.add(minPortLbl);
                    minPortPanel.add(minPortField);
                    topPanel.add(minPortPanel);
                }
                {
                    /*
                     * Maximum port number:
                     */
                    maxPort = prefs.getInt(MAX_PORT,
                            PortNumberSet.MAX_PORT_NUMBER);
                    final JLabel maxPortLbl = new JLabel("Max:");
                    maxPortField = new JTextField(Integer.toString(
                            PortNumberSet.MAX_PORT_NUMBER).length());
                    maxPortField.setText(Integer.toString(maxPort));
                    maxPortField.setToolTipText("Maximum potential port (1 - "
                            + PortNumberSet.MAX_PORT_NUMBER + ")");
                    final JPanel maxPortPanel = new JPanel();
                    new BoxLayout(maxPortPanel, BoxLayout.X_AXIS);
                    maxPortPanel.add(maxPortLbl);
                    maxPortPanel.add(maxPortField);
                    topPanel.add(maxPortPanel);
                }
            }
            {
                /*
                 * Received file count:
                 */
                final JLabel receivedFileCountLbl = new JLabel(
                        "Received Files:");
                topPanel.add(receivedFileCountLbl);
                topPanel.add(receivedFileCountField, "span");
            }
            {
                /*
                 * Peer count:
                 */
                final JLabel peerCountLbl = new JLabel("Peer Count:");
                topPanel.add(peerCountLbl);
                topPanel.add(peerCountField, "span");
            }
        }
        {
            /*
             * Bottom panel:
             */
            botPanel.setLayout(new MigLayout("wrap 2"));
            {
                /*
                 * Cancel button:
                 */
                final JButton cancelButton = new JButton("Cancel");
                cancelButton.setMnemonic('C');
                cancelButton.setToolTipText("Cancel program execution");
                cancelButton.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(final ActionEvent e) {
                        doneListener.actionPerformed(null);
                    }
                });
                botPanel.add(cancelButton,
                        new CC().alignX("right").sizeGroup("1"));
                /*
                 * Execute button:
                 */
                executeButton = new JButton("OK");
                executeButton.setMnemonic('O');
                executeButton.setToolTipText("Execute program");
                executeButton.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(final ActionEvent e) {
                        execute();
                    }
                });
                botPanel.add(executeButton,
                        new CC().alignX("right").sizeGroup("1"));
            }
        }

        container.setLayout(new MigLayout("wrap 1"));
        container.add(topPanel, "center");
        container.add(botPanel, "align right");
    }

    /**
     * Sets the initial focus. This method should be called after the components
     * are realized (e.g., by {@link JFrame#pack()}) but before the containing
     * window is displayed (e.g., by {@link JFrame#setVisible(boolean)}).
     */
    private void setInitialFocus() {
        executeButton.requestFocusInWindow();
    }

    /**
     * Allows the user to choose a file (regular or directory). Shall execute on
     * the AWT EDT.
     * 
     * @param title
     *            The title for the dialog window or {@code null}.
     * @param toolTip
     *            The tool-tip for the "approve" button or {@code null}.
     * @param label
     *            The widget whose text will be set to the result.
     * @param options
     *            {@link JFileChooser} file selection mode options.
     * @throws AssertionError
     *             if the current thread isn't the AWT EDT.
     * @throws NullPointerException
     *             if {@code label == null}.
     */
    @SuppressWarnings("serial")
    private void chooseFile(final String title, final String toolTip,
            final JLabel label, final int options) {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new AssertionError();
        }
        final JDialog dialog = new JDialog(
                SwingUtilities.getWindowAncestor(container), title);
        final JFileChooser actionsChooser = new JFileChooser() {
            @Override
            public void approveSelection() {
                final File file = getSelectedFile();
                dialog.dispose();
                label.setText(file.toString());
                final Window window = SwingUtilities.getWindowAncestor(label);
                if (null != window) {
                    window.pack();
                }
            }

            @Override
            public void cancelSelection() {
                dialog.dispose();
            }
        };
        actionsChooser.setFileSelectionMode(options);
        actionsChooser.setMultiSelectionEnabled(false);
        actionsChooser.setAcceptAllFileFilterUsed(false);
        actionsChooser.setApproveButtonToolTipText(toolTip);
        actionsChooser.setSelectedFile(new File(label.getText()));
        dialog.setModal(true);
        dialog.add(actionsChooser);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setLocationRelativeTo(container);
        dialog.pack();
        dialog.setVisible(true);
    }

    /**
     * Executes a {@link SubscriberState} on the AWT event dispatch thread.
     * 
     * @throws AssertionError
     *             if {@code !SwingUtilities.isEventDispatchThread()}.
     */
    private void execute() {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new AssertionError();
        }
        archiveChangeButton.setEnabled(false);
        actionsChangeButton.setEnabled(false);
        minPort = Integer.parseInt(minPortField.getText());
        maxPort = Integer.parseInt(maxPortField.getText());
        try {
            prefs.put(ROOT_DIR, archiveDir.toString());
            prefs.putInt(MIN_PORT, minPort);
            prefs.putInt(MAX_PORT, maxPort);
            try {
                prefs.flush();
            }
            catch (final BackingStoreException e) {
                logger.error("Unable to save preferences", e);
            }
            botPanel.removeAll();
            subscriber = new Subscriber(archiveDir.toPath(),
                    subscription.getTrackerAddress(),
                    subscription.getPredicate(), new Processor());
            final JButton terminateButton = new JButton("Terminate");
            terminateButton.setMnemonic('T');
            terminateButton.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(final ActionEvent e) {
                    cancel();
                }
            });
            botPanel.setLayout(new MigLayout("wrap 1"));
            botPanel.add(terminateButton, "align right");
            updateTask = new UpdateTask();
            updateTask.addPropertyChangeListener(new PropertyChangeListener() {
                @Override
                public void propertyChange(final PropertyChangeEvent evt) {
                    if (RECEIVED_FILE_COUNT.equals(evt.getPropertyName())) {
                        receivedFileCountField.setText(evt.getNewValue()
                                .toString());
                    }
                    else if (PEER_COUNT.equals(evt.getPropertyName())) {
                        peerCountField.setText(evt.getNewValue().toString());
                    }
                }
            }); // received on AWT EDT
            updateTask.execute();
            subscriberTask = new SubscriberTask();
            new Thread(subscriberTask).start();
        }
        catch (final Throwable t) {
            logger.error("Fatal error", t);
            JOptionPane.showMessageDialog(null, "I'm sorry, but I "
                    + "encountered the following fatal error:\n" + t,
                    getClass().getSimpleName(), JOptionPane.ERROR_MESSAGE);
            doneListener.actionPerformed(null);
        }
    }

    /**
     * Cancels execution. Executes on the AWT event dispatch thread.
     */
    void cancel() {
        if (null != updateTask) {
            updateTask.cancel(true);
        }
        if (null != subscriberTask) {
            if (!subscriberTask.cancel(true)) {
                try {
                    subscriberTask.get();
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    logger.error("Execution failure in subscriber task", cause);
                    throw Util.launderThrowable(cause);
                }
                catch (final InterruptedException ignored) {
                }
            }
        }
        doneListener.actionPerformed(null);
    }

    /**
     * Sets the location of the GUI.
     */
    private static void setLocation(final JFrame frame) {
        final int xPosition = prefs.getInt(X_POSITION, -1);
        final int yPosition = prefs.getInt(Y_POSITION, -1);
        if (-1 == xPosition || -1 == yPosition) {
            frame.setLocationByPlatform(true);
        }
        else {
            frame.setLocation(new Point(xPosition, yPosition));
        }
    }

    /**
     * Saves the location of the GUI.
     */
    private static void saveLocation(final JFrame frame) {
        final Point location = frame.getLocation();
        prefs.putInt(X_POSITION, location.x);
        prefs.putInt(Y_POSITION, location.y);
        try {
            prefs.flush();
        }
        catch (final BackingStoreException e) {
            logger.error("Unable to save window location preference", e);
        }
    }

    /**
     * Creates the GUI and displays it. Shall execute on the AWT event dispatch
     * thread (EDT).
     * 
     * @param pathname
     *            Pathname of the subscription file.
     * @throws AssertionError
     *             if the current thread isn't the AWT EDT.
     */
    private static void createGuiAndDisplay(final String pathname) {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new AssertionError();
        }
        try {
            final Path path = Paths.get(pathname);
            final JFrame frame = new JFrame(SubscriberApp.class.getSimpleName());
            frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
            final SubscriberApp app = new SubscriberApp(path,
                    frame.getContentPane(), new ActionListener() {
                        @Override
                        public void actionPerformed(final ActionEvent e) {
                            saveLocation(frame);
                            frame.dispose();
                            System.exit(0);
                        }
                    });
            frame.addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosing(final WindowEvent e) {
                    app.cancel();
                    saveLocation(frame);
                    frame.dispose();
                    System.exit(0);
                }
            });
            setLocation(frame);
            frame.pack();
            app.setInitialFocus();
            frame.setVisible(true);
        }
        catch (final Throwable t) {
            logger.error("Fatal error", t);
            JOptionPane.showMessageDialog(null, "I'm sorry, but I "
                    + "encountered the following fatal error:\n" + t,
                    SubscriberApp.class.getSimpleName(),
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    /**
     * Launches an instance of this application.
     * 
     * @param args
     *            [0] The pathname of subscription file (optional).
     * @throws UnsupportedLookAndFeelException
     *             if the system's native L&F isn't supported (rather unlikely).
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    public static void main(final String[] args) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException,
            UnsupportedLookAndFeelException {
        if (args.length == 0) {
            JOptionPane.showMessageDialog(null, "Installed",
                    SubscriberApp.class.getSimpleName(),
                    JOptionPane.INFORMATION_MESSAGE);
        }
        else {
            String path = null;

            if (args.length == 1) {
                path = args[0];
            }
            else if (args.length == 2) {
                if ("-open".equals(args[0]) || "-print".equals(args[0])) {
                    path = args[1];
                }
            }

            if (null == path) {
                logger.error("Invalid invocation: wrong number of arguments: "
                        + args.length);
            }
            else {
                final String pathname = path;
                UIManager.setLookAndFeel(UIManager
                        .getSystemLookAndFeelClassName());
                javax.swing.SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        createGuiAndDisplay(pathname);
                    }
                });
            }
        }
    }
}
