package org.kettle.beam.core.util;

import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.awt.*;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Web {

    public static void open(BaseStepDialog dialog, String url) {
        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            try {
                Desktop.getDesktop().browse(new URI(url));
            } catch (Exception ex) {;
                dialog.logError("Ocorreu um erro inesperado em Web.open(). Exception : ", ex);
                Logger.getLogger(dialog.getClass().getName()).log(Level.SEVERE, "Ocorreu um erro inesperado em Web.open().", ex);
            }
        }
    }

}
