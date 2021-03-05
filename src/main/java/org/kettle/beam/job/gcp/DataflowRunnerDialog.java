package org.kettle.beam.job.gcp;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class DataflowRunnerDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private static Class<?> PKG = DataflowRunnerDialog.class;

    private Text wName;
    private TextVar wProjectId;
    private ComboVar wRegion;
    private TextVar wZone;
    private TextVar wJobName;
    private TextVar wTemplateLocation;
    private TextVar wTempLocation;
    private TextVar wWaitService;

    private Button wOK;
    private Button wCancel;

    private VariableSpace space;

    private DataflowRunnerMeta meta;

    private boolean changed;

    private int margin;

    public DataflowRunnerDialog(Shell parent, JobEntryInterface jobEntryInterface, Repository repository, JobMeta jobMeta ) {
        super( parent, jobEntryInterface, repository, jobMeta );
        meta = (DataflowRunnerMeta)jobEntryInt;
        this.parent = parent;
        props = PropsUI.getInstance();

        space = new Variables();
        space.initializeVariablesFrom( null );

        if ( this.meta.getName() == null ) {
            this.meta.setName( BaseMessages.getString( PKG, "DataflowRunnerDialog.Default.Name" ) );
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

        shell.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.Shell.Title" ) );
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
        wlName.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.Name" ) );
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

        //Project ID
        //
        Label wlProjectId = new Label( shell, SWT.RIGHT );
        wlProjectId.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.ProjectId" ) );
        props.setLook( wlProjectId );
        FormData fdlProjectId = new FormData();
        fdlProjectId.left = new FormAttachment( 0, 0 );
        fdlProjectId.top = new FormAttachment( lastControl, margin );
        fdlProjectId.right = new FormAttachment( middle, -margin );
        wlProjectId.setLayoutData( fdlProjectId );
        wProjectId = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wProjectId );
        FormData fdProjectId = new FormData();
        fdProjectId.left = new FormAttachment( middle, 0 );
        fdProjectId.top = new FormAttachment( wlProjectId, 0, SWT.CENTER );
        fdProjectId.right = new FormAttachment( 95, 0 );
        wProjectId.setLayoutData( fdProjectId );
        lastControl = wProjectId;

        //Region
        //
        Label wlRegion = new Label( shell, SWT.RIGHT );
        props.setLook( wlRegion );
        wlRegion.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.Region" ) );
        FormData fdlRegion = new FormData();
        fdlRegion.top = new FormAttachment( lastControl, margin );
        fdlRegion.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlRegion.right = new FormAttachment( middle, -margin );
        wlRegion.setLayoutData( fdlRegion );
        wRegion = new ComboVar( space, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wRegion );
        wRegion.setItems( BeamConst.getGcpRegionNames() );
        FormData fdRegion = new FormData();
        fdRegion.top = new FormAttachment( wlRegion, 0, SWT.CENTER );
        fdRegion.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdRegion.right = new FormAttachment( 95, 0 );
        wRegion.setLayoutData( fdRegion );
        lastControl = wRegion;

        //Zone
        //
        Label wlZone = new Label( shell, SWT.RIGHT );
        props.setLook( wlZone );
        wlZone.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.Zone" ) );
        FormData fdlZone = new FormData();
        fdlZone.top = new FormAttachment( lastControl, margin );
        fdlZone.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlZone.right = new FormAttachment( middle, -margin );
        wlZone.setLayoutData( fdlZone );
        wZone = new TextVar( space, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wZone );
        FormData fdZone = new FormData();
        fdZone.top = new FormAttachment( wlZone, 0, SWT.CENTER );
        fdZone.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdZone.right = new FormAttachment( 95, 0 );
        wZone.setLayoutData( fdZone );
        lastControl = wZone;

        //Job Name
        //
        Label wlJobName = new Label( shell, SWT.RIGHT );
        props.setLook( wlJobName );
        wlJobName.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.JobName" ) );
        FormData fdlJobName = new FormData();
        fdlJobName.top = new FormAttachment( lastControl, margin );
        fdlJobName.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlJobName.right = new FormAttachment( middle, -margin );
        wlJobName.setLayoutData( fdlJobName );
        wJobName = new TextVar( space, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wJobName );
        FormData fdJobName = new FormData();
        fdJobName.top = new FormAttachment( wlJobName, 0, SWT.CENTER );
        fdJobName.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdJobName.right = new FormAttachment( 95, 0 );
        wJobName.setLayoutData( fdJobName );
        lastControl = wJobName;

        //Template Location
        //
        Label wlTemplateLocation = new Label( shell, SWT.RIGHT );
        wlTemplateLocation.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.TemplateLocation" ) );
        props.setLook( wlTemplateLocation );
        FormData fdlTemplateLocation = new FormData();
        fdlTemplateLocation.left = new FormAttachment( 0, 0 );
        fdlTemplateLocation.top = new FormAttachment( lastControl, margin );
        fdlTemplateLocation.right = new FormAttachment( middle, -margin );
        wlTemplateLocation.setLayoutData( fdlTemplateLocation );
        wTemplateLocation = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wTemplateLocation );
        FormData fdTemplateLocation = new FormData();
        fdTemplateLocation.left = new FormAttachment( middle, 0 );
        fdTemplateLocation.top = new FormAttachment( wlTemplateLocation, 0, SWT.CENTER );
        fdTemplateLocation.right = new FormAttachment( 95, 0 );
        wTemplateLocation.setLayoutData( fdTemplateLocation );
        lastControl = wTemplateLocation;

        //Temp Location
        //
        Label wlTempLocation = new Label( shell, SWT.RIGHT );
        wlTempLocation.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.TempLocation" ) );
        props.setLook( wlTempLocation );
        FormData fdlTempLocation = new FormData();
        fdlTempLocation.left = new FormAttachment( 0, 0 );
        fdlTempLocation.top = new FormAttachment( lastControl, margin );
        fdlTempLocation.right = new FormAttachment( middle, -margin );
        wlTempLocation.setLayoutData( fdlTempLocation );
        wTempLocation = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wTempLocation );
        FormData fdTempLocation = new FormData();
        fdTempLocation.left = new FormAttachment( middle, 0 );
        fdTempLocation.top = new FormAttachment( wlTempLocation, 0, SWT.CENTER );
        fdTempLocation.right = new FormAttachment( 95, 0 );
        wTempLocation.setLayoutData( fdTempLocation );
        lastControl = wTempLocation;

        //Wait Service
        //
        Label wlWaitService = new Label( shell, SWT.RIGHT );
        wlWaitService.setText( BaseMessages.getString( PKG, "DataflowRunnerDialog.WaitService" ) );
        props.setLook( wlWaitService );
        FormData fdlWaitService = new FormData();
        fdlWaitService.left = new FormAttachment( 0, 0 );
        fdlWaitService.top = new FormAttachment( lastControl, margin );
        fdlWaitService.right = new FormAttachment( middle, -margin );
        wlWaitService.setLayoutData( fdlWaitService );
        wWaitService = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wWaitService );
        FormData fdWaitService = new FormData();
        fdWaitService.left = new FormAttachment( middle, 0 );
        fdWaitService.top = new FormAttachment( wlWaitService, 0, SWT.CENTER );
        fdWaitService.right = new FormAttachment( 95, 0 );
        wWaitService.setLayoutData( fdWaitService );
        lastControl = wWaitService;


        wName.addSelectionListener( selAdapter );
        wProjectId.addSelectionListener( selAdapter );
        wRegion.addSelectionListener( selAdapter );
        wZone.addSelectionListener( selAdapter );
        wJobName.addSelectionListener( selAdapter );
        wTemplateLocation.addSelectionListener( selAdapter );
        wTempLocation.addSelectionListener( selAdapter );
        wWaitService.addSelectionListener( selAdapter );


        FocusListener focusListener = new FocusListener() {
            @Override public void focusGained(FocusEvent focusEvent) {ensureWaitServiceUrl();}
            @Override public void focusLost(FocusEvent focusEvent) {ensureWaitServiceUrl();}
            private void ensureWaitServiceUrl(){
                if(!Strings.isNullOrEmpty(wWaitService.getText())){return;}
                if(Strings.isNullOrEmpty(wProjectId.getText()) || Strings.isNullOrEmpty(wRegion.getText())){return;}
                String url = "https://" + wRegion.getText() + "-" + wProjectId.getText() + ".cloudfunctions.net/wait-execution";
                wWaitService.setText(url);
            }
        };
        wProjectId.addFocusListener(focusListener);
        wRegion.addFocusListener(focusListener);
        wWaitService.addFocusListener(focusListener);

        wRegion.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent modifyEvent) {
                if(Strings.isNullOrEmpty(wZone.getText()) || !wZone.getText().startsWith(wRegion.getText())){
                    wZone.setText(wRegion.getText() + "-a");
                }
            }
        });


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
        wProjectId.setText(Const.NVL(meta.getProjectId(), ""));
        wRegion.setText(Const.NVL(meta.getRegion(), ""));
        wZone.setText(Const.NVL(meta.getZone(), ""));
        wJobName.setText(Const.NVL(meta.getJobName(), ""));
        wTemplateLocation.setText(Const.NVL(meta.getTemplateLocation(), ""));
        wTempLocation.setText(Const.NVL(meta.getTempLocation(), ""));
        wWaitService.setText(Const.NVL(meta.getWaitService(), ""));
        wName.selectAll();
    }

    private void cancel() {
        meta.setChanged( changed );
        meta = null;
        dispose();
    }

    private void ok() {
        try {
            if (Strings.isNullOrEmpty(wName.getText())) {throw new Exception("Nome não informado.");}
            if (Strings.isNullOrEmpty(wProjectId.getText()) ) {throw new Exception("Projeto nao informado.");}
            if (Strings.isNullOrEmpty(wRegion.getText()) ) {throw new Exception("Regiao nao informado.");}
            if (Strings.isNullOrEmpty(wZone.getText()) ) {throw new Exception("Zona nao informada.");}
            if (Strings.isNullOrEmpty(wJobName.getText()) ) {throw new Exception("Job nao informado.");}
            if (Strings.isNullOrEmpty(wTemplateLocation.getText()) ) {throw new Exception("Json nao informado.");}
            if (Strings.isNullOrEmpty(wTempLocation.getText()) ) {throw new Exception("Caminho temporario nao informado.");}
            if (Strings.isNullOrEmpty(wWaitService.getText()) ) {throw new Exception("Servico de Espera nao informado.");}
            if(!wZone.getText().startsWith(wRegion.getText()) ) {throw new Exception("Zona fora da regiao.");}
            meta.setName( wName.getText() );
            meta.setProjectId( wProjectId.getText() );
            meta.setRegion( wRegion.getText() );
            meta.setZone( wZone.getText() );
            meta.setJobName( wJobName.getText() );
            meta.setTemplateLocation( wTemplateLocation.getText() );
            meta.setTempLocation( wTempLocation.getText() );
            meta.setWaitService( wWaitService.getText() );
            meta.setChanged();
            dispose();

        }catch (Exception ex){
            SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

        }

    }
}