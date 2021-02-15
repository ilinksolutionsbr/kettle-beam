package org.kettle.beam.job.dataflow;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class DataFlowRunnerDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private static Class<?> PKG = DataFlowRunnerDialog.class;

    private Text wName;
    private TextVar wJobName;

    private Button wOK;
    private Button wCancel;

    private VariableSpace space;

    private DataFlowRunnerMeta meta;

    private boolean changed;

    private int margin;

    public DataFlowRunnerDialog(Shell parent, JobEntryInterface jobEntryInterface, Repository repository, JobMeta jobMeta ) {
        super( parent, jobEntryInterface, repository, jobMeta );
        meta = (DataFlowRunnerMeta)jobEntryInt;
        this.parent = parent;
        props = PropsUI.getInstance();

        space = new Variables();
        space.initializeVariablesFrom( null );

        if ( this.meta.getName() == null ) {
            this.meta.setName( BaseMessages.getString( PKG, "DataFlowRunnerDialog.Default.Name" ) );
        }
    }

    public JobEntryInterface open() {
        Display display = parent.getDisplay();
        shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        props.setLook( shell );
        shell.setImage( GUIResource.getInstance().getImageSlave() );
        JobDialog.setShellImage( shell, meta );

        changed = meta.hasChanged();

        int middle = props.getMiddlePct();
        margin = Const.MARGIN + 2;

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setText( BaseMessages.getString( PKG, "DataFlowRunnerDialog.Shell.Title" ) );
        shell.setLayout( formLayout );


        wOK = new Button( shell, SWT.PUSH );
        wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        wOK.addListener( SWT.Selection, e -> ok() );

        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
        wCancel.addListener( SWT.Selection, e -> cancel() );

        BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );


        ModifyListener lsMod = e -> meta.setChanged();
        SelectionAdapter selAdapter = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                ok();
            }
        };


        Label wlName = new Label( shell, SWT.RIGHT );
        props.setLook( wlName );
        wlName.setText( BaseMessages.getString( PKG, "DataFlowRunnerDialog.Name" ) );
        FormData fdlName = new FormData();
        fdlName.top = new FormAttachment( 0, margin );
        fdlName.left = new FormAttachment( 0, -margin );
        fdlName.right = new FormAttachment( middle, -margin );
        wlName.setLayoutData( fdlName );
        wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        wName.addModifyListener( lsMod );
        props.setLook( wName );
        FormData fdName = new FormData();
        fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
        fdName.left = new FormAttachment( middle, 0 );
        fdName.right = new FormAttachment( 95, 0 );
        wName.setLayoutData( fdName );
        Control lastControl = wName;

        Label wlJobName = new Label( shell, SWT.RIGHT );
        wlJobName.setText( BaseMessages.getString( PKG, "DataFlowRunnerDialog.JobName" ) );
        props.setLook( wlJobName );
        FormData fdlProjectId = new FormData();
        fdlProjectId.left = new FormAttachment( 0, 0 );
        fdlProjectId.top = new FormAttachment( lastControl, margin );
        fdlProjectId.right = new FormAttachment( middle, -margin );
        wlJobName.setLayoutData( fdlProjectId );
        wJobName = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wJobName );
        FormData fdProjectId = new FormData();
        fdProjectId.left = new FormAttachment( middle, 0 );
        fdProjectId.top = new FormAttachment( wlJobName, 0, SWT.CENTER );
        fdProjectId.right = new FormAttachment( 100, 0 );
        wJobName.setLayoutData( fdProjectId );
        lastControl = wJobName;


        wName.addSelectionListener( selAdapter );
        wJobName.addSelectionListener( selAdapter );


        meta.setChanged( changed );

        shell.addShellListener( new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                cancel();
            }
        } );

        getData();

        BaseStepDialog.setSize( shell );

        shell.open();

        while ( !shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }

        return meta;
    }

    private void dispose() {
        WindowProperty winprop = new WindowProperty( shell );
        props.setScreen( winprop );
        shell.dispose();
    }

    public void getData() {
        wName.setText(Const.NVL(meta.getName(), ""));
        wJobName.setText(Const.NVL(meta.getJobName(), ""));
        wName.selectAll();
    }

    private void cancel() {
        meta.setChanged( changed );
        meta = null;
        dispose();
    }

    private void ok() {
        meta.setName( wName.getText() );
        meta.setJobName( wJobName.getText() );
        meta.setChanged();
        dispose();
    }
}