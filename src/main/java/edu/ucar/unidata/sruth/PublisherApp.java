/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Point;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.WindowConstants;

import net.miginfocom.layout.CC;
import net.miginfocom.swing.MigLayout;

import org.slf4j.Logger;

/**
 * Graphical user interface to a {@link Publisher}.
 * 
 * @author Steven R. Emmerson
 */
public final class PublisherApp {
    /**
     * The display update task.
     * 
     * @author Steven R. Emmerson
     */
    private final class UpdateTask extends SwingWorker<Void, Void> {
        @Override
        protected Void doInBackground() {
            int clientCount = 0;
            while (!isCancelled()) {
                try {
                    Thread.sleep(1000);
                }
                catch (final InterruptedException e) {
                    break; // treat as termination request
                }
                final int oldClientCount = clientCount;
                clientCount = publisher.getClientCount();
                firePropertyChange(CLIENT_COUNT, oldClientCount, clientCount);
            }
            return null;
        }
    }

    /**
     * The {@link Publisher} execution task.
     * 
     * If this class subclasses {@link SwingWorker}, then the {@link Publisher}
     * object won't be executed for some reason (perhaps because this object
     * stays in the {@link SwingWorkder} queue).
     * 
     * @author Steven R. Emmerson
     */
    private final class PublisherTask extends FutureTask<Void> {
        PublisherTask() {
            super(publisher);
        }

        @Override
        public void run() {
            try {
                publisher.call();
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
     * The logger for this class.
     */
    private static final Logger      logger           = Util.getLogger();
    /**
     * The class-specific user preferences.
     */
    private static final Preferences prefs            = Preferences
                                                              .userNodeForPackage(PublisherApp.class);
    /**
     * The name of the root directory preference.
     */
    private static final String      ROOT_DIR         = "rootDir";
    /**
     * The name of the minimum candidate port number preference.
     */
    private static final String      MIN_PORT         = "minPort";
    /**
     * The name of the maximum candidate port number preference.
     */
    private static final String      MAX_PORT         = "maxPort";
    /**
     * The name of the x-position preference.
     */
    private static final String      X_POSITION       = "xPosition";
    /**
     * The name of the y-position preference.
     */
    private static final String      Y_POSITION       = "yPosition";
    /**
     * The name of the property that's the number of connected clients.
     */
    private static final String      CLIENT_COUNT     = "clientCount";
    /**
     * The pathname of the root directory of the archive.
     */
    private File                     rootDir;
    /**
     * The minimum candidate port number.
     */
    private int                      minPort          = 0;
    /**
     * The maximum candidate port number.
     */
    private int                      maxPort          = 0;
    /**
     * The port-selection panel.
     */
    private JPanel                   portSelectionPanel;
    /**
     * The top panel.
     */
    private final JPanel             topPanel         = new JPanel();
    /**
     * The bottom panel.
     */
    private final JPanel             botPanel         = new JPanel();
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
    private JLabel                   rootDirField;
    /**
     * The button for changing the pathname of the archive.
     */
    private JButton                  changeButton;
    /**
     * The button for executing the program.
     */
    private JButton                  executeButton;
    /**
     * The number of received files.
     */
    private final JLabel             clientCountField = new JLabel("0");
    /**
     * The updating task.
     */
    private UpdateTask               updateTask;
    /**
     * The Publisher.
     */
    private Publisher                publisher;
    /**
     * The publisher task.
     */
    private PublisherTask            publisherTask;
    /**
     * The input field for the minimum port number.
     */
    private JTextField               minPortField;
    /**
     * The input field for the maximum port number.
     */
    private JTextField               maxPortField;
    /**
     * The radio button for choosing ephemeral port numbers.
     */
    private JRadioButton             ephemeralRadioButton;
    /**
     * The radio button for letting the user choose the port numbers.
     */
    private JRadioButton             portRangeRadioButton;

    /**
     * Constructs from the pathname of a subscription file.
     * 
     * @param container
     *            The AWT container in which to put widgets.
     * @param doneListener
     *            The listener to call when this instance is done.
     * @throws NullPointerException
     *             if {@code container == null || doneListener == null}.
     */
    PublisherApp(final Container container, final ActionListener doneListener) {
        if (null == container || null == doneListener) {
            throw new NullPointerException();
        }
        this.container = container;
        this.doneListener = doneListener;

        topPanel.setLayout(new MigLayout("wrap 2"));
        topPanel.add(new JLabel("Archive:"), "align right");
        topPanel.add(getRootDirPanel());
        topPanel.add(new JLabel("Port Numbers:"), "align right");
        topPanel.add(getPortSelectionPanel());
        topPanel.add(new JLabel("Client Count:"), "align right");
        topPanel.add(clientCountField);

        botPanel.setLayout(new MigLayout("wrap 2"));
        botPanel.add(getCancelButton(), new CC().alignX("right").sizeGroup("1"));
        botPanel.add(getExecuteButton(), new CC().alignX("right")
                .sizeGroup("1"));

        container.setLayout(new MigLayout("wrap 1"));
        container.add(topPanel, "center");
        container.add(botPanel, "align right");
    }

    private Component getExecuteButton() {
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
        return executeButton;
    }

    private Component getCancelButton() {
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
        return cancelButton;
    }

    private Component getPortSelectionPanel() {
        /*
         * Port numbers selection:
         */
        minPort = prefs.getInt(MIN_PORT, PortNumberSet.MIN_PORT_NUMBER);
        maxPort = prefs.getInt(MAX_PORT, PortNumberSet.MAX_PORT_NUMBER);

        minPortField = new JTextField(Integer.toString(
                PortNumberSet.MAX_PORT_NUMBER).length());
        minPortField.setText(Integer.toString(minPort));
        minPortField.setToolTipText("Minimum potential port number ("
                + PortNumberSet.MIN_PORT_NUMBER + ")");
        minPortField.setEnabled(minPort != 0);

        maxPortField = new JTextField(Integer.toString(
                PortNumberSet.MAX_PORT_NUMBER).length());
        maxPortField.setText(Integer.toString(maxPort));
        maxPortField.setToolTipText("Maximum potential port number ("
                + (PortNumberSet.MAX_PORT_NUMBER) + ")");
        maxPortField.setEnabled(minPortField.isEnabled());

        ephemeralRadioButton = new JRadioButton("Assigned by O/S", minPort == 0);
        ephemeralRadioButton
                .setToolTipText("Operating-system chooses random ports");
        ephemeralRadioButton.setMnemonic('A');
        final JPanel ephemeralPanel = new JPanel();
        ephemeralPanel.add(ephemeralRadioButton);

        portRangeRadioButton = new JRadioButton("Within range", minPort != 0);
        portRangeRadioButton.setToolTipText("User chooses port range");
        portRangeRadioButton.setMnemonic('W');
        portRangeRadioButton.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(final ItemEvent e) {
                minPortField.setEnabled(e.getStateChange() == ItemEvent.SELECTED);
                maxPortField.setEnabled(minPortField.isEnabled());
            }
        });

        final ButtonGroup portButtonGroup = new ButtonGroup();
        portButtonGroup.add(ephemeralRadioButton);
        portButtonGroup.add(portRangeRadioButton);

        final JPanel userPortPanel = new JPanel();
        new BoxLayout(userPortPanel, BoxLayout.X_AXIS);
        userPortPanel.add(portRangeRadioButton);
        userPortPanel.add(minPortField);
        userPortPanel.add(new JLabel("through"));
        userPortPanel.add(maxPortField);

        portSelectionPanel = new JPanel();
        portSelectionPanel.setLayout(new MigLayout("wrap 1"));
        portSelectionPanel.add(ephemeralPanel);
        portSelectionPanel.add(userPortPanel);
        portSelectionPanel.setBorder(BorderFactory
                .createLineBorder(Color.BLACK));

        return portSelectionPanel;
    }

    private Component getRootDirPanel() {
        /*
         * Archive directory selection:
         */
        final String rootDirPref = prefs.get(ROOT_DIR,
                System.getProperty("user.home") + File.separator
                        + this.getClass().getSimpleName());
        rootDir = new File(rootDirPref);
        rootDirField = new JLabel(rootDir.toString());

        changeButton = new JButton("Change...");
        changeButton.setMnemonic('h');
        changeButton.setToolTipText("Choose archive directory");
        changeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                chooseRootDir();
            }
        });

        final JPanel rootDirPanel = new JPanel();
        new BoxLayout(rootDirPanel, BoxLayout.X_AXIS);
        rootDirPanel.add(rootDirField);
        rootDirPanel.add(changeButton);

        return rootDirPanel;
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
        final JDialog dialog = new JDialog(
                SwingUtilities.getWindowAncestor(container),
                "Choose Archive Directory");
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
     * Executes this instance on the AWT event dispatch thread.
     * 
     * @throws AssertionError
     *             if {@code !SwingUtilities.isEventDispatchThread()}.
     */
    private void execute() {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new AssertionError();
        }
        changeButton.setEnabled(false);
        ephemeralRadioButton.setEnabled(false);
        portRangeRadioButton.setEnabled(false);
        minPortField.setEnabled(false);
        maxPortField.setEnabled(false);
        prefs.put(ROOT_DIR, rootDir.toString());
        if (ephemeralRadioButton.isSelected()) {
            minPort = 0;
            maxPort = 0;
        }
        else {
            minPort = Integer.parseInt(minPortField.getText());
            maxPort = Integer.parseInt(maxPortField.getText());
        }
        prefs.putInt(MIN_PORT, minPort);
        prefs.putInt(MAX_PORT, maxPort);
        try {
            prefs.flush();
        }
        catch (final BackingStoreException e) {
            logger.error("Unable to save preferences", e);
        }
        botPanel.removeAll();
        try {
            publisher = new Publisher(rootDir.toPath());
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
                    clientCountField.setText(evt.getNewValue().toString());
                }
            }); // received on AWT EDT
            updateTask.execute();
            publisherTask = new PublisherTask();
            new Thread(publisherTask).start();
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
        if (null != publisherTask) {
            if (!publisherTask.cancel(true)) {
                try {
                    publisherTask.get();
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    logger.error("Execution failure in publisher task", cause);
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
     * @throws AssertionError
     *             if the current thread isn't the AWT EDT.
     */
    private static void createGuiAndDisplay() {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new AssertionError();
        }
        try {
            final JFrame frame = new JFrame(PublisherApp.class.getSimpleName());
            frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
            final PublisherApp app = new PublisherApp(frame.getContentPane(),
                    new ActionListener() {
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
                    PublisherApp.class.getSimpleName(),
                    JOptionPane.ERROR_MESSAGE);
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
        }
    }

    /**
     * Launches an instance of this application.
     * 
     * @param args
     *            Program arguments
     * 
     * @throws UnsupportedLookAndFeelException
     *             if the system's native L&F isn't supported (rather unlikely).
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    public static void main(final String[] args) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException,
            UnsupportedLookAndFeelException {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createGuiAndDisplay();
            }
        });
    }
}
