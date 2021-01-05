package org.kettle.beam.core;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.ui.core.dialog.ErrorDialog;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class KettleErrorDialog extends ErrorDialog {

    public KettleErrorDialog(Shell parent, String title, String message, Throwable throwable) {
        super(parent, title, message, getThrowable(throwable));
    }

    public KettleErrorDialog(Shell parent, String title, String message, Throwable throwable, Function<String, String> exMsgFunction) {
        super(parent, title, message, getThrowable(throwable), exMsgFunction);
    }

    public KettleErrorDialog(Shell parent, String title, String message, Exception exception) {
        super(parent, title, message, getException(exception));
    }

    public KettleErrorDialog(Shell parent, String title, String message, Exception exception, boolean showCancelButton) {
        super(parent, title, message, getException(exception), showCancelButton);
    }


    private static Throwable getThrowable(Throwable ex){
        return getException(ex);
    }

    private static Exception getException(Throwable ex){
        String message = getMessage(ex);
        return new Exception(message, ex);
    }

    private static String getMessage(Throwable ex){
        StringBuilder builder = new StringBuilder();
        mountMessage(builder, ex, new ArrayList<>());
        return builder.toString();
    }
    private static void mountMessage(StringBuilder builder, Throwable ex, List<Throwable> list){
        if(ex == null){return;}
        if(list.contains(ex) || list.size() >= 20){return;}
        builder.append("---------------------\n");
        builder.append(ex.getMessage() + "\n");
        list.add(ex);
        mountMessage(builder, ex.getCause(), list);
    }

}
