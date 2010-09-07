import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JRootPane;
import javax.swing.UIManager;

public final class TestApp {
    private ActionListener listener;

    TestApp(final JRootPane rootPane) {
        try {
            final JFileChooser chooser = new JFileChooser();
            chooser.addActionListener(new ActionListener() {
                public void actionPerformed(final ActionEvent e) {
                    System.out.println("Action=" + e.paramString());
                    System.out.println("Selected File="
                            + chooser.getSelectedFile());
                    if (null != listener) {
                        listener.actionPerformed(new ActionEvent(this,
                                ActionEvent.ACTION_PERFORMED, "done"));
                    }
                }
            });
            rootPane.getContentPane().add(chooser);
        }
        catch (final Throwable t) {
            t.printStackTrace();
        }
    }

    void setActionListener(final ActionListener listener) {
        this.listener = listener;
    }

    private static void createGuiAndDisplay() {
        final JFrame frame = new JFrame();
        frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        final TestApp app = new TestApp(frame.getRootPane());
        app.setActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                frame.dispose();
                System.exit(0);
            }
        });
        frame.pack();
        frame.setVisible(true);
    }

    public static void main(final String[] args) throws Exception {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createGuiAndDisplay();
            }
        });
    }
}
