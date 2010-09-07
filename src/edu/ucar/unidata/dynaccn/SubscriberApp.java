/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.WindowConstants;
import javax.xml.parsers.ParserConfigurationException;

import net.miginfocom.layout.CC;
import net.miginfocom.swing.MigLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
            long count = 0;
            while (!isCancelled()) {
                try {
                    Thread.sleep(1000);
                }
                catch (final InterruptedException e) {
                    break; // treat as termination request
                }
                final long oldCount = count;
                count = subscriber.getReceivedFileCount();
                firePropertyChange(RECEIVED_FILE_COUNT, oldCount, count);
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
    private static final Logger      logger                 = LoggerFactory
                                                                    .getLogger(SubscriberApp.class);
    /**
     * The user preferences.
     */
    private static final Preferences prefs                  = Preferences
                                                                    .userNodeForPackage(SubscriberApp.class);
    /**
     * The name of the root directory preference.
     */
    private static final String      ROOT_DIR               = "rootDir";
    /**
     * The name of the x-position preference.
     */
    private static final String      X_POSITION             = "xPosition";
    /**
     * The name of the y-position preference.
     */
    private static final String      Y_POSITION             = "yPosition";
    /**
     * The name of the property that's the number of received files.
     */
    private static final String      RECEIVED_FILE_COUNT    = "receivedFileCount";
    /**
     * The pathname of the root directory of the archive.
     */
    private File                     rootDir;
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
    private final JLabel             rootDirField;
    /**
     * The button for changing the pathname of the archive.
     */
    private final JButton            changeButton;
    /**
     * The button for executing the program.
     */
    private final JButton            executeButton;
    /**
     * The number of received files.
     */
    private final JLabel             receivedFileCountField = new JLabel("0");
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
     *             if {@code path == null || rootPane == null || doneListener ==
     *             null}.
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
        subscription = new Subscription(path);
        final JLabel trackerLbl = new JLabel("Tracker:");
        final JLabel trackerField = new JLabel(subscription.getTrackerAddress()
                .toString());
        final JLabel predicateLbl = new JLabel("Predicate:");
        final JLabel predicateField = new JLabel(subscription.getPredicate()
                .toString());
        final String rootDirPref = prefs.get(ROOT_DIR, System
                .getProperty("user.home")
                + File.pathSeparator + "subscriptionApp");
        rootDir = new File(rootDirPref);
        final JLabel rootDirLbl = new JLabel("Archive:");
        rootDirField = new JLabel(rootDir.toString());
        final JLabel receivedFileCountLbl = new JLabel("Received Files:");

        changeButton = new JButton("Change...");
        changeButton.setMnemonic('h');
        changeButton.setToolTipText("Choose archive directory");
        changeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                chooseRootDir();
            }
        });

        final JButton cancelButton = new JButton("Cancel");
        cancelButton.setMnemonic('C');
        cancelButton.setToolTipText("Cancel program execution");
        cancelButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                doneListener.actionPerformed(null);
            }
        });

        executeButton = new JButton("OK");
        executeButton.setMnemonic('O');
        executeButton.setToolTipText("Execute program");
        executeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                execute();
            }
        });

        topPanel.setLayout(new MigLayout("wrap 3"));
        topPanel.add(trackerLbl, "align right");
        topPanel.add(trackerField, "span");
        topPanel.add(predicateLbl, "align right");
        topPanel.add(predicateField, "span");
        topPanel.add(rootDirLbl, "align right");
        topPanel.add(rootDirField);
        topPanel.add(changeButton);
        topPanel.add(receivedFileCountLbl);
        topPanel.add(receivedFileCountField, "span");
        botPanel.setLayout(new MigLayout("wrap 2"));
        botPanel.add(cancelButton, new CC().alignX("right").sizeGroup("1"));
        botPanel.add(executeButton, new CC().alignX("right").sizeGroup("1"));
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
     * Allows the user to choose the pathname for the archive. Shall execute on
     * the AWT EDT.
     * 
     * @throws AssertionError
     *             if the current thread isn't the AWT EDT.
     */
    @SuppressWarnings("serial")
    private void chooseRootDir() {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new AssertionError();
        }
        final JDialog dialog = new JDialog(SwingUtilities
                .getWindowAncestor(container), "Choose Archive Directory");
        final JFileChooser rootDirChooser = new JFileChooser() {
            @Override
            public void approveSelection() {
                rootDir = getSelectedFile();
                dialog.dispose();
                rootDirField.setText(rootDir.toString());
                final Window window = SwingUtilities
                        .getWindowAncestor(rootDirField);
                if (null != window) {
                    window.pack();
                }
            }

            @Override
            public void cancelSelection() {
                dialog.dispose();
            }
        };
        rootDirChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        rootDirChooser.setMultiSelectionEnabled(false);
        rootDirChooser.setAcceptAllFileFilterUsed(false);
        rootDirChooser
                .setApproveButtonToolTipText("Use selected directory for data archive");
        rootDirChooser.setSelectedFile(rootDir);
        dialog.setModal(true);
        dialog.add(rootDirChooser);
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
        changeButton.setEnabled(false);
        prefs.put(ROOT_DIR, rootDir.toString());
        try {
            prefs.flush();
        }
        catch (final BackingStoreException e) {
            logger.error("Unable to save archive location preference", e);
        }
        botPanel.removeAll();
        try {
            subscriber = new Subscriber(rootDir.toPath(), subscription
                    .getTrackerAddress(), subscription.getPredicate());
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
                    receivedFileCountField
                            .setText(evt.getNewValue().toString());
                }
            }); // received on AWT EDT
            updateTask.execute();
            subscriberTask = new SubscriberTask();
            new Thread(subscriberTask).start();
        }
        catch (final IOException e) {
            logger.error("Fatal error", e);
            JOptionPane.showMessageDialog(null, "I'm sorry, but I "
                    + "encountered the following fatal error:\n" + e,
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
            if (!subscriberTask.isDone()) {
                subscriberTask.cancel(true);
            }
            else {
                try {
                    subscriberTask.get();
                }
                catch (final ExecutionException e) {
                    logger.error("Execution failure in subscriber task", e
                            .getCause());
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
            final SubscriberApp app = new SubscriberApp(path, frame
                    .getContentPane(), new ActionListener() {
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
