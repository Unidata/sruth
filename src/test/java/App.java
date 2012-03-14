/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

import javax.swing.JFrame;
import javax.swing.JOptionPane;

@SuppressWarnings("serial")
class App extends JFrame {
    App() {
        super("App");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        JOptionPane.showMessageDialog(this, "App works");
        pack();
    }

    public static void main(final String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                new App().setVisible(true);
            }
        });
    }
}
