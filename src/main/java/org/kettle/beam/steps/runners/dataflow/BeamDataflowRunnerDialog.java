package org.kettle.beam.steps.runners.dataflow;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.KettleErrorDialog;
import org.kettle.beam.pipeline.fatjar.FatJarBuilder;
import org.kettle.beam.steps.runners.RunnerParameter;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.List;

/**
 * Classe responsável por criar a janela de configuração do step
 * Dataflow Runner.
 *
 */
public class BeamDataflowRunnerDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = BeamDataflowRunnerDialog.class;

    public BeamDataflowRunnerMeta meta;

    private CTabFolder wTabFolder;

    private CTabItem wGeneralTab;
    private CTabItem wParametersTab;
    private CTabItem wDataflowTab;

    private ScrolledComposite wGeneralSComp;
    private ScrolledComposite wDataflowSComp;

    private Composite wGeneralComp;
    private Composite wParametersComp;
    private Composite wDataflowComp;

    private TextVar wUserAgent;
    private TextVar wTempLocation;
    private TextVar wStreamingKettleStepsFlushInterval;
    private TextVar wPluginsToStage;
    private TextVar wStepPluginClasses;
    private TextVar wXpPluginClasses;
    private TextVar wFatJar;

    private TableView wParameters;

    private TextVar wGcpProjectId;
    private TextVar wGcpAppName;
    private TextVar wGcpStagingLocation;
    private TextVar wGcpTemplateLocation;
    private TextVar wGcpNetwork;
    private TextVar wGcpSubNetwork;
    private TextVar wGcpInitialNumberOfWorkers;
    private TextVar wGcpMaximumNumberOfWorkers;
    private TextVar wGcpAutoScalingAlgorithm;
    private ComboVar wGcpWorkerMachineType;
    private TextVar wGcpWorkerDiskType;
    private TextVar wGcpDiskSizeGb;
    private ComboVar wGcpRegion;
    private TextVar wGcpZone;
    private Button wGcpStreaming;

    private Button wOK;
    private Button wCancel;

    private VariableSpace space;

    private int margin;
    private boolean ok;

    public BeamDataflowRunnerDialog(Shell parent, Object in, TransMeta transMeta, String sname ) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        meta = (BeamDataflowRunnerMeta) in;
    }

    public String open() {
        Shell parent = this.getParent();
        Display display = parent.getDisplay();
        this.shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        this.props.setLook( this.shell );
        setShellImage(this.shell, this.meta);

        this.changed = this.meta.hasChanged();

        int middle = this.props.getMiddlePct();
        margin = Const.MARGIN + 2;

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.Shell.Title" ) );
        shell.setLayout( formLayout );

        // Buttons at the bottom of the dialo...
        //
        wOK = new Button( shell, SWT.PUSH );
        wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        wOK.addListener( SWT.Selection, e -> ok() );

        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
        wCancel.addListener( SWT.Selection, e -> cancel() );

        BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

        // The rest of the dialog is for the widgets...
        //
        addFormWidgets();

        // Add listeners

        SelectionAdapter selAdapter = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                ok();
            }
        };
        wStepname.addSelectionListener( selAdapter );
        wUserAgent.addSelectionListener( selAdapter );
        wTempLocation.addSelectionListener( selAdapter );
        wPluginsToStage.addSelectionListener( selAdapter );
        wStepPluginClasses.addSelectionListener( selAdapter );
        wXpPluginClasses.addSelectionListener( selAdapter );
        wFatJar.addSelectionListener( selAdapter );
        wStreamingKettleStepsFlushInterval.addSelectionListener( selAdapter );
        wGcpProjectId.addSelectionListener( selAdapter );
        wGcpAppName.addSelectionListener( selAdapter );
        wGcpStagingLocation.addSelectionListener( selAdapter );
        wGcpTemplateLocation.addSelectionListener( selAdapter );
        wGcpNetwork.addSelectionListener( selAdapter );
        wGcpSubNetwork.addSelectionListener( selAdapter );
        wGcpInitialNumberOfWorkers.addSelectionListener( selAdapter );
        wGcpMaximumNumberOfWorkers.addSelectionListener( selAdapter );
        wGcpStreaming.addSelectionListener( selAdapter );
        wGcpAutoScalingAlgorithm.addSelectionListener( selAdapter );
        wGcpWorkerMachineType.addSelectionListener( selAdapter );
        wGcpWorkerDiskType.addSelectionListener( selAdapter );
        wGcpDiskSizeGb.addSelectionListener( selAdapter );
        wGcpRegion.addSelectionListener( selAdapter );
        wGcpZone.addSelectionListener( selAdapter );

        // Detect X or ALT-F4 or something that kills this window...
        this.shell.addShellListener( new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                cancel();
            }
        } );

        getData();

        BaseStepDialog.setSize( this.shell );

        this.meta.setChanged(this.changed);

        this.shell.open();

        while ( !this.shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }
        return stepname;
    }

    private void addFormWidgets() {

        int middle = Const.MIDDLE_PCT;

        // Stepname line
        wlStepname = new Label( shell, SWT.RIGHT );
        wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
        props.setLook( wlStepname );
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment( 0, 0 );
        fdlStepname.top = new FormAttachment( 0, margin );
        fdlStepname.right = new FormAttachment( middle, -margin );
        wlStepname.setLayoutData( fdlStepname );
        wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        wStepname.setText( stepname );
        props.setLook( wStepname );
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment( middle, 0 );
        fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
        fdStepname.right = new FormAttachment( 100, 0 );
        wStepname.setLayoutData( fdStepname );
        Control lastControl = wStepname;

        wTabFolder = new CTabFolder( shell, SWT.BORDER );
        props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
        wTabFolder.setSimple( false );
        FormData fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment( 0, 0 );
        fdTabFolder.right = new FormAttachment( 100, 0 );
        fdTabFolder.top = new FormAttachment( lastControl, margin * 2 );
        fdTabFolder.bottom = new FormAttachment( wOK, -margin * 2 );
        wTabFolder.setLayoutData( fdTabFolder );

        addGeneralTab();
        addParametersTab();
        addDataflowTab();

        wTabFolder.setSelection( 0 );

    }

    private void addGeneralTab() {

        int middle = Const.MIDDLE_PCT;

        wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
        wGeneralTab.setText( "  General  " );

        wGeneralSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
        wGeneralSComp.setLayout( new FillLayout() );

        wGeneralComp = new Composite( wGeneralSComp, SWT.COLOR_WIDGET_BACKGROUND );
        props.setLook( wGeneralComp );

        FormLayout generalLayout = new FormLayout();
        generalLayout.marginWidth = 10;
        generalLayout.marginHeight = 10;
        wGeneralComp.setLayout( generalLayout );

        // UserAgent
        //
        Label wlUserAgent = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlUserAgent );
        wlUserAgent.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.UserAgent.Label" ) );
        FormData fdlUserAgent = new FormData();
        fdlUserAgent.top = new FormAttachment( 0, 0 );
        fdlUserAgent.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlUserAgent.right = new FormAttachment( middle, -margin );
        wlUserAgent.setLayoutData( fdlUserAgent );
        wUserAgent = new TextVar( transMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wUserAgent );
        FormData fdUserAgent = new FormData();
        fdUserAgent.top = new FormAttachment( wlUserAgent, 0, SWT.CENTER );
        fdUserAgent.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdUserAgent.right = new FormAttachment( 95, 0 );
        wUserAgent.setLayoutData( fdUserAgent );
        Control lastControl = wUserAgent;

        // TempLocation
        //
        Label wlTempLocation = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlTempLocation );
        wlTempLocation.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.TempLocation.Label" ) );
        FormData fdlTempLocation = new FormData();
        fdlTempLocation.top = new FormAttachment( lastControl, margin );
        fdlTempLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlTempLocation.right = new FormAttachment( middle, -margin );
        wlTempLocation.setLayoutData( fdlTempLocation );
        wTempLocation = new TextVar( transMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wTempLocation );
        FormData fdTempLocation = new FormData();
        fdTempLocation.top = new FormAttachment( wlTempLocation, 0, SWT.CENTER );
        fdTempLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdTempLocation.right = new FormAttachment( 95, 0 );
        wTempLocation.setLayoutData( fdTempLocation );
        lastControl = wTempLocation;

        // PluginsToStage
        //
        Label wlPluginsToStage = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlPluginsToStage );
        wlPluginsToStage.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.PluginsToStage.Label" ) );
        FormData fdlPluginsToStage = new FormData();
        fdlPluginsToStage.top = new FormAttachment( lastControl, margin );
        fdlPluginsToStage.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlPluginsToStage.right = new FormAttachment( middle, -margin );
        wlPluginsToStage.setLayoutData( fdlPluginsToStage );
        wPluginsToStage = new TextVar( transMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wPluginsToStage );
        FormData fdPluginsToStage = new FormData();
        fdPluginsToStage.top = new FormAttachment( wlPluginsToStage, 0, SWT.CENTER );
        fdPluginsToStage.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdPluginsToStage.right = new FormAttachment( 95, 0 );
        wPluginsToStage.setLayoutData( fdPluginsToStage );
        lastControl = wPluginsToStage;

        // StepPluginClasses
        //
        Label wlStepPluginClasses = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlStepPluginClasses );
        wlStepPluginClasses.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.StepPluginClasses.Label" ) );
        FormData fdlStepPluginClasses = new FormData();
        fdlStepPluginClasses.top = new FormAttachment( lastControl, margin );
        fdlStepPluginClasses.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlStepPluginClasses.right = new FormAttachment( middle, -margin );
        wlStepPluginClasses.setLayoutData( fdlStepPluginClasses );
        Button wbStepPluginClasses = new Button( wGeneralComp, SWT.PUSH );
        wStepPluginClasses = new TextVar( transMeta, wGeneralComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.WRAP );
        props.setLook( wStepPluginClasses );
        FormData fdStepPluginClasses = new FormData();
        fdStepPluginClasses.top = new FormAttachment( lastControl, margin );
        fdStepPluginClasses.bottom = new FormAttachment( lastControl, 200 );
        fdStepPluginClasses.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdStepPluginClasses.right = new FormAttachment( 95, 0 );
        wStepPluginClasses.setLayoutData( fdStepPluginClasses );
        wbStepPluginClasses.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.StepPluginClasses.Button" ) );
        FormData fdbStepPluginClasses = new FormData();
        fdbStepPluginClasses.top = new FormAttachment( lastControl, margin );
        fdbStepPluginClasses.left = new FormAttachment( wStepPluginClasses, margin );
        fdbStepPluginClasses.right = new FormAttachment( 100, 0 );
        wbStepPluginClasses.setLayoutData( fdbStepPluginClasses );
        wbStepPluginClasses.addListener( SWT.Selection, this::findStepClasses );
        lastControl = wStepPluginClasses;

        // XpPluginClasses
        //
        Label wlXpPluginClasses = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlXpPluginClasses );
        wlXpPluginClasses.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.XpPluginClasses.Label" ) );
        FormData fdlXpPluginClasses = new FormData();
        fdlXpPluginClasses.top = new FormAttachment( lastControl, margin );
        fdlXpPluginClasses.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlXpPluginClasses.right = new FormAttachment( middle, -margin );
        wlXpPluginClasses.setLayoutData( fdlXpPluginClasses );
        Button wbXpPluginClasses = new Button( wGeneralComp, SWT.PUSH );
        wXpPluginClasses = new TextVar( transMeta, wGeneralComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.WRAP );
        props.setLook( wXpPluginClasses );
        FormData fdXpPluginClasses = new FormData();
        fdXpPluginClasses.top = new FormAttachment( lastControl, margin );
        fdXpPluginClasses.bottom = new FormAttachment( lastControl, 300 );
        fdXpPluginClasses.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdXpPluginClasses.right = new FormAttachment( 95, 0 );
        wXpPluginClasses.setLayoutData( fdXpPluginClasses );
        wbXpPluginClasses.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.XpPluginClasses.Button" ) );
        FormData fdbXpPluginClasses = new FormData();
        fdbXpPluginClasses.top = new FormAttachment( lastControl, margin );
        fdbXpPluginClasses.left = new FormAttachment( wXpPluginClasses, margin );
        fdbXpPluginClasses.right = new FormAttachment( 100, 0 );
        wbXpPluginClasses.setLayoutData( fdbXpPluginClasses );
        wbXpPluginClasses.addListener( SWT.Selection, this::findXpClasses );
        lastControl = wXpPluginClasses;

        // FatJar
        //
        Label wlFatJar = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlFatJar );
        wlFatJar.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.FatJar.Label" ) );
        FormData fdlFatJar = new FormData();
        fdlFatJar.top = new FormAttachment( lastControl, margin );
        fdlFatJar.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlFatJar.right = new FormAttachment( middle, -margin );
        wlFatJar.setLayoutData( fdlFatJar );
        wFatJar = new TextVar( transMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wFatJar );
        FormData fdFatJar = new FormData();
        fdFatJar.top = new FormAttachment( wlFatJar, 0, SWT.CENTER );
        fdFatJar.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdFatJar.right = new FormAttachment( 95, 0 );
        wFatJar.setLayoutData( fdFatJar );
        Button wbFatJar = new Button( wGeneralComp, SWT.PUSH );
        wbFatJar.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.FatJar.Button" ) );
        FormData fdbFatJar = new FormData();
        fdbFatJar.top = new FormAttachment( lastControl, margin );
        fdbFatJar.left = new FormAttachment( wFatJar, margin );
        fdbFatJar.right = new FormAttachment( 100, 0 );
        wbFatJar.setLayoutData( fdbFatJar );
        wbFatJar.addListener( SWT.Selection, this::buildFatJar );
        lastControl = wFatJar;

        // Streaming Kettle Steps Flush Interval
        //
        Label wlStreamingKettleStepsFlushInterval = new Label( wGeneralComp, SWT.RIGHT );
        props.setLook( wlStreamingKettleStepsFlushInterval );
        wlStreamingKettleStepsFlushInterval.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.StreamingKettleStepsFlushInterval.Label" ) );
        FormData fdlStreamingKettleStepsFlushInterval = new FormData();
        fdlStreamingKettleStepsFlushInterval.top = new FormAttachment( lastControl, margin );
        fdlStreamingKettleStepsFlushInterval.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlStreamingKettleStepsFlushInterval.right = new FormAttachment( middle, -margin );
        wlStreamingKettleStepsFlushInterval.setLayoutData( fdlStreamingKettleStepsFlushInterval );
        wStreamingKettleStepsFlushInterval = new TextVar( transMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wStreamingKettleStepsFlushInterval );
        FormData fdStreamingKettleStepsFlushInterval = new FormData();
        fdStreamingKettleStepsFlushInterval.top = new FormAttachment( wlStreamingKettleStepsFlushInterval, 0, SWT.CENTER );
        fdStreamingKettleStepsFlushInterval.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdStreamingKettleStepsFlushInterval.right = new FormAttachment( 95, 0 );
        wStreamingKettleStepsFlushInterval.setLayoutData( fdStreamingKettleStepsFlushInterval );
        lastControl = wStreamingKettleStepsFlushInterval;

        FormData fdGeneralComp = new FormData();
        fdGeneralComp.left = new FormAttachment( 0, 0 );
        fdGeneralComp.top = new FormAttachment( 0, 0 );
        fdGeneralComp.right = new FormAttachment( 100, 0 );
        fdGeneralComp.bottom = new FormAttachment( 100, 0 );
        wGeneralComp.setLayoutData( fdGeneralComp );

        wGeneralComp.pack();
        Rectangle bounds = wGeneralComp.getBounds();

        wGeneralSComp.setContent( wGeneralComp );
        wGeneralSComp.setExpandHorizontal( true );
        wGeneralSComp.setExpandVertical( true );
        wGeneralSComp.setMinWidth( bounds.width );
        wGeneralSComp.setMinHeight( bounds.height );

        wGeneralTab.setControl( wGeneralSComp );
    }

    private void buildFatJar( Event event ) {
        try {
            VariableSpace space = Variables.getADefaultVariableSpace();

            BeamDataflowRunnerMeta metaConfig = new BeamDataflowRunnerMeta();
            getInfo( metaConfig );

            FileDialog dialog = new FileDialog( shell, SWT.SAVE );
            dialog.setText( "Select the location of the Kettle+Beam+Plugins fat jar" );
            dialog.setFilterNames( new String[] { "Jar files (*.jar)", "All Files (*.*)" } );
            dialog.setFilterExtensions( new String[] { "*.jar", "*.*" } ); // Windows
            if ( StringUtils.isNotEmpty( metaConfig.getFatJar() ) ) {
                dialog.setFileName( space.environmentSubstitute( metaConfig.getFatJar() ) );
            }
            String filename = dialog.open();
            if ( StringUtils.isEmpty( filename ) ) {
                return;
            }

            List<String> files = BeamConst.findLibraryFilesToStage( null, metaConfig.getPluginsToStage(), true, true );
            files.removeIf( s -> s.contains( "commons-logging" ) || s.startsWith( "log4j" ) || s.contains( "xml-apis" ) );

            // Find the plugin classes for the specified plugins...
            //
            String stepPluginClasses = findPluginClasses( Step.class.getName() );
            if (StringUtils.isNotEmpty(metaConfig.getStepPluginClasses())) {
                if (StringUtils.isEmpty( stepPluginClasses )) {
                    stepPluginClasses="";
                } else {
                    stepPluginClasses+=",";
                }
                stepPluginClasses+=metaConfig.getStepPluginClasses();
            }
            String xpPluginClasses = findPluginClasses( ExtensionPoint.class.getName() );
            if (StringUtils.isNotEmpty(metaConfig.getXpPluginClasses())) {
                if (StringUtils.isEmpty( xpPluginClasses )) {
                    xpPluginClasses="";
                } else {
                    xpPluginClasses+=",";
                }
                xpPluginClasses+=metaConfig.getStepPluginClasses();
            }


            FatJarBuilder fatJarBuilder = new FatJarBuilder( filename, files );
            fatJarBuilder.setExtraStepPluginClasses( stepPluginClasses );
            fatJarBuilder.setExtraXpPluginClasses( xpPluginClasses );
            Cursor waitCursor = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
            Cursor regularCursor = shell.getCursor();

            try {
                shell.setCursor( waitCursor );
                fatJarBuilder.buildTargetJar();
            } finally {
                shell.setCursor( regularCursor );
                waitCursor.dispose();
            }

            // All went well, insert the filename...
            //
            wFatJar.setText( filename );

        } catch ( Exception e ) {
            new KettleErrorDialog( shell, "Error", "Error building fat jar:", e );
        }
    }

    private void findStepClasses( Event event ) {
        String stepPluginClasses = findPluginClasses( Step.class.getName() );
        if ( stepPluginClasses != null ) {
            wStepPluginClasses.setText( stepPluginClasses );
        }
    }

    private void findXpClasses( Event event ) {
        String xpPluginClasses = findPluginClasses( ExtensionPoint.class.getName() );
        if ( xpPluginClasses != null ) {
            wXpPluginClasses.setText( xpPluginClasses );
        }
    }

    private String findPluginClasses( String pluginClassName ) {
        BeamDataflowRunnerMeta metaConfig = new BeamDataflowRunnerMeta();
        getInfo( metaConfig );

        try {
            return FatJarBuilder.findPluginClasses( pluginClassName, metaConfig.getPluginsToStage() );
        } catch ( Exception e ) {
            new KettleErrorDialog( shell, "Error", "Error find plugin classes of annotation type '" + pluginClassName, e );
            return null;
        }
    }

    private void addParametersTab() {
        wParametersTab = new CTabItem( wTabFolder, SWT.NONE );
        wParametersTab.setText( "Parameters" );

        wParametersComp = new Composite( wTabFolder, SWT.COLOR_WIDGET_BACKGROUND );
        props.setLook( wParametersComp );
        wParametersComp.setLayout( new FormLayout() );

        ColumnInfo[] columnInfos = new ColumnInfo[] {
                new ColumnInfo( "Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
                new ColumnInfo( "Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        };

        wParameters = new TableView( transMeta, wParametersComp, SWT.BORDER, columnInfos, meta.getParameters().size(), null, props );
        props.setLook( wParameters );
        FormData fdParameters = new FormData();
        fdParameters.left = new FormAttachment( 0, 0 );
        fdParameters.right = new FormAttachment( 100, 0 );
        fdParameters.top = new FormAttachment( 0, 0 );
        fdParameters.bottom = new FormAttachment( 100, 0 );
        wParameters.setLayoutData( fdParameters );

        FormData fdParametersComp = new FormData();
        fdParametersComp.left = new FormAttachment( 0, 0 );
        fdParametersComp.right = new FormAttachment( 100, 0 );
        fdParametersComp.top = new FormAttachment( 0, 0 );
        fdParametersComp.bottom = new FormAttachment( 100, 0 );
        wParametersComp.setLayoutData( fdParametersComp );

        wParametersTab.setControl( wParametersComp );
    }

    private void addDataflowTab() {

        int middle = Const.MIDDLE_PCT;

        wDataflowTab = new CTabItem( wTabFolder, SWT.NONE );
        wDataflowTab.setText( "  Dataflow  " );

        wDataflowSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
        wDataflowSComp.setLayout( new FillLayout() );

        wDataflowComp = new Composite( wDataflowSComp, SWT.NONE );
        props.setLook( wDataflowComp );

        FormLayout fileLayout = new FormLayout();
        fileLayout.marginWidth = 3;
        fileLayout.marginHeight = 3;
        wDataflowComp.setLayout( fileLayout );

        // Project ID
        //
        Label wlGcpProjectId = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpProjectId );
        wlGcpProjectId.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpProjectId.Label" ) );
        FormData fdlGcpProjectId = new FormData();
        fdlGcpProjectId.top = new FormAttachment( 0, 0 );
        fdlGcpProjectId.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpProjectId.right = new FormAttachment( middle, -margin );
        wlGcpProjectId.setLayoutData( fdlGcpProjectId );
        wGcpProjectId = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpProjectId );
        FormData fdGcpProjectId = new FormData();
        fdGcpProjectId.top = new FormAttachment( wlGcpProjectId, 0, SWT.CENTER );
        fdGcpProjectId.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpProjectId.right = new FormAttachment( 95, 0 );
        wGcpProjectId.setLayoutData( fdGcpProjectId );
        Control lastControl = wGcpProjectId;

        // App name
        //
        Label wlGcpAppName = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpAppName );
        wlGcpAppName.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpAppName.Label" ) );
        FormData fdlGcpAppName = new FormData();
        fdlGcpAppName.top = new FormAttachment( lastControl, margin );
        fdlGcpAppName.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpAppName.right = new FormAttachment( middle, -margin );
        wlGcpAppName.setLayoutData( fdlGcpAppName );
        wGcpAppName = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpAppName );
        FormData fdGcpAppName = new FormData();
        fdGcpAppName.top = new FormAttachment( wlGcpAppName, 0, SWT.CENTER );
        fdGcpAppName.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpAppName.right = new FormAttachment( 95, 0 );
        wGcpAppName.setLayoutData( fdGcpAppName );
        lastControl = wGcpAppName;

        // Staging location
        //
        Label wlGcpStagingLocation = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpStagingLocation );
        wlGcpStagingLocation.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpStagingLocation.Label" ) );
        FormData fdlGcpStagingLocation = new FormData();
        fdlGcpStagingLocation.top = new FormAttachment( lastControl, margin );
        fdlGcpStagingLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpStagingLocation.right = new FormAttachment( middle, -margin );
        wlGcpStagingLocation.setLayoutData( fdlGcpStagingLocation );
        wGcpStagingLocation = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpStagingLocation );
        FormData fdGcpStagingLocation = new FormData();
        fdGcpStagingLocation.top = new FormAttachment( wlGcpStagingLocation, 0, SWT.CENTER );
        fdGcpStagingLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpStagingLocation.right = new FormAttachment( 95, 0 );
        wGcpStagingLocation.setLayoutData( fdGcpStagingLocation );
        lastControl = wGcpStagingLocation;


        // Template location
        //
        Label wlGcpTemplateLocation = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpTemplateLocation );
        wlGcpTemplateLocation.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpTemplateLocation.Label" ) );
        FormData fdlGcpTemplateLocation = new FormData();
        fdlGcpTemplateLocation.top = new FormAttachment( lastControl, margin );
        fdlGcpTemplateLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpTemplateLocation.right = new FormAttachment( middle, -margin );
        wlGcpTemplateLocation.setLayoutData( fdlGcpTemplateLocation );
        wGcpTemplateLocation = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpTemplateLocation );
        FormData fdGcpTemplateLocation = new FormData();
        fdGcpTemplateLocation.top = new FormAttachment( wlGcpTemplateLocation, 0, SWT.CENTER );
        fdGcpTemplateLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpTemplateLocation.right = new FormAttachment( 95, 0 );
        wGcpTemplateLocation.setLayoutData( fdGcpTemplateLocation );
        lastControl = wGcpTemplateLocation;




        // Network
        //
        Label wlGcpNetwork = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpNetwork );
        wlGcpNetwork.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpNetwork.Label" ) );
        FormData fdlGcpNetwork = new FormData();
        fdlGcpNetwork.top = new FormAttachment( lastControl, margin );
        fdlGcpNetwork.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpNetwork.right = new FormAttachment( middle, -margin );
        wlGcpNetwork.setLayoutData( fdlGcpNetwork );
        wGcpNetwork = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpNetwork );
        FormData fdGcpNetwork = new FormData();
        fdGcpNetwork.top = new FormAttachment( wlGcpNetwork, 0, SWT.CENTER );
        fdGcpNetwork.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpNetwork.right = new FormAttachment( 95, 0 );
        wGcpNetwork.setLayoutData( fdGcpNetwork );
        lastControl = wGcpNetwork;

        // SubNetwork
        //
        Label wlGcpSubNetwork = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpSubNetwork );
        wlGcpSubNetwork.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpSubNetwork.Label" ) );
        FormData fdlGcpSubNetwork = new FormData();
        fdlGcpSubNetwork.top = new FormAttachment( lastControl, margin );
        fdlGcpSubNetwork.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpSubNetwork.right = new FormAttachment( middle, -margin );
        wlGcpSubNetwork.setLayoutData( fdlGcpSubNetwork );
        wGcpSubNetwork = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpSubNetwork );
        FormData fdGcpSubNetwork = new FormData();
        fdGcpSubNetwork.top = new FormAttachment( wlGcpSubNetwork, 0, SWT.CENTER );
        fdGcpSubNetwork.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpSubNetwork.right = new FormAttachment( 95, 0 );
        wGcpSubNetwork.setLayoutData( fdGcpSubNetwork );
        lastControl = wGcpSubNetwork;


        // Initial number of workers
        //
        Label wlInitialNumberOfWorkers = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlInitialNumberOfWorkers );
        wlInitialNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpInitialNumberOfWorkers.Label" ) );
        FormData fdlInitialNumberOfWorkers = new FormData();
        fdlInitialNumberOfWorkers.top = new FormAttachment( lastControl, margin );
        fdlInitialNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlInitialNumberOfWorkers.right = new FormAttachment( middle, -margin );
        wlInitialNumberOfWorkers.setLayoutData( fdlInitialNumberOfWorkers );
        wGcpInitialNumberOfWorkers = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpInitialNumberOfWorkers );
        FormData fdInitialNumberOfWorkers = new FormData();
        fdInitialNumberOfWorkers.top = new FormAttachment( wlInitialNumberOfWorkers, 0, SWT.CENTER );
        fdInitialNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdInitialNumberOfWorkers.right = new FormAttachment( 95, 0 );
        wGcpInitialNumberOfWorkers.setLayoutData( fdInitialNumberOfWorkers );
        lastControl = wGcpInitialNumberOfWorkers;

        // Maximum number of workers
        //
        Label wlMaximumNumberOfWorkers = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlMaximumNumberOfWorkers );
        wlMaximumNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpMaximumNumberOfWorkers.Label" ) );
        FormData fdlMaximumNumberOfWorkers = new FormData();
        fdlMaximumNumberOfWorkers.top = new FormAttachment( lastControl, margin );
        fdlMaximumNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlMaximumNumberOfWorkers.right = new FormAttachment( middle, -margin );
        wlMaximumNumberOfWorkers.setLayoutData( fdlMaximumNumberOfWorkers );
        wGcpMaximumNumberOfWorkers = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpMaximumNumberOfWorkers );
        FormData fdMaximumNumberOfWorkers = new FormData();
        fdMaximumNumberOfWorkers.top = new FormAttachment( wlMaximumNumberOfWorkers, 0, SWT.CENTER );
        fdMaximumNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdMaximumNumberOfWorkers.right = new FormAttachment( 95, 0 );
        wGcpMaximumNumberOfWorkers.setLayoutData( fdMaximumNumberOfWorkers );
        lastControl = wGcpMaximumNumberOfWorkers;

        // Auto Scaling Algorithm
        //
        Label wlAutoScalingAlgorithm = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlAutoScalingAlgorithm );
        wlAutoScalingAlgorithm.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpAutoScalingAlgorithm.Label" ) );
        FormData fdlAutoScalingAlgorithm = new FormData();
        fdlAutoScalingAlgorithm.top = new FormAttachment( lastControl, margin );
        fdlAutoScalingAlgorithm.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlAutoScalingAlgorithm.right = new FormAttachment( middle, -margin );
        wlAutoScalingAlgorithm.setLayoutData( fdlAutoScalingAlgorithm );
        wGcpAutoScalingAlgorithm = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpAutoScalingAlgorithm );
        FormData fdAutoScalingAlgorithm = new FormData();
        fdAutoScalingAlgorithm.top = new FormAttachment( wlAutoScalingAlgorithm, 0, SWT.CENTER );
        fdAutoScalingAlgorithm.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdAutoScalingAlgorithm.right = new FormAttachment( 95, 0 );
        wGcpAutoScalingAlgorithm.setLayoutData( fdAutoScalingAlgorithm );
        lastControl = wGcpAutoScalingAlgorithm;


        // Worker machine type
        //
        Label wlGcpWorkerMachineType = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpWorkerMachineType );
        wlGcpWorkerMachineType.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpWorkerMachineType.Label" ) );
        FormData fdlGcpWorkerMachineType = new FormData();
        fdlGcpWorkerMachineType.top = new FormAttachment( lastControl, margin );
        fdlGcpWorkerMachineType.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpWorkerMachineType.right = new FormAttachment( middle, -margin );
        wlGcpWorkerMachineType.setLayoutData( fdlGcpWorkerMachineType );
        wGcpWorkerMachineType = new ComboVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpWorkerMachineType );
        wGcpWorkerMachineType.setItems( BeamConst.getGcpWorkerMachineTypeDescriptions() );
        FormData fdGcpWorkerMachineType = new FormData();
        fdGcpWorkerMachineType.top = new FormAttachment( wlGcpWorkerMachineType, 0, SWT.CENTER );
        fdGcpWorkerMachineType.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpWorkerMachineType.right = new FormAttachment( 95, 0 );
        wGcpWorkerMachineType.setLayoutData( fdGcpWorkerMachineType );
        lastControl = wGcpWorkerMachineType;

        // Worker disk type
        //
        Label wlGcpWorkerDiskType = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpWorkerDiskType );
        wlGcpWorkerDiskType.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpWorkerDiskType.Label" ) );
        FormData fdlGcpWorkerDiskType = new FormData();
        fdlGcpWorkerDiskType.top = new FormAttachment( lastControl, margin );
        fdlGcpWorkerDiskType.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpWorkerDiskType.right = new FormAttachment( middle, -margin );
        wlGcpWorkerDiskType.setLayoutData( fdlGcpWorkerDiskType );
        wGcpWorkerDiskType = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpWorkerDiskType );
        FormData fdGcpWorkerDiskType = new FormData();
        fdGcpWorkerDiskType.top = new FormAttachment( wlGcpWorkerDiskType, 0, SWT.CENTER );
        fdGcpWorkerDiskType.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpWorkerDiskType.right = new FormAttachment( 95, 0 );
        wGcpWorkerDiskType.setLayoutData( fdGcpWorkerDiskType );
        lastControl = wGcpWorkerDiskType;

        // Disk Size in GB
        //
        Label wlGcpDiskSizeGb = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpDiskSizeGb );
        wlGcpDiskSizeGb.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpDiskSizeGb.Label" ) );
        FormData fdlGcpDiskSizeGb = new FormData();
        fdlGcpDiskSizeGb.top = new FormAttachment( lastControl, margin );
        fdlGcpDiskSizeGb.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpDiskSizeGb.right = new FormAttachment( middle, -margin );
        wlGcpDiskSizeGb.setLayoutData( fdlGcpDiskSizeGb );
        wGcpDiskSizeGb = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpDiskSizeGb );
        FormData fdGcpDiskSizeGb = new FormData();
        fdGcpDiskSizeGb.top = new FormAttachment( wlGcpDiskSizeGb, 0, SWT.CENTER );
        fdGcpDiskSizeGb.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpDiskSizeGb.right = new FormAttachment( 95, 0 );
        wGcpDiskSizeGb.setLayoutData( fdGcpDiskSizeGb );
        lastControl = wGcpDiskSizeGb;

        // Region
        //
        Label wlGcpRegion = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpRegion );
        wlGcpRegion.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpRegion.Label" ) );
        FormData fdlGcpRegion = new FormData();
        fdlGcpRegion.top = new FormAttachment( lastControl, margin );
        fdlGcpRegion.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpRegion.right = new FormAttachment( middle, -margin );
        wlGcpRegion.setLayoutData( fdlGcpRegion );
        wGcpRegion = new ComboVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpRegion );
        wGcpRegion.setItems( BeamConst.getGcpRegionDescriptions() );
        FormData fdGcpRegion = new FormData();
        fdGcpRegion.top = new FormAttachment( wlGcpRegion, 0, SWT.CENTER );
        fdGcpRegion.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpRegion.right = new FormAttachment( 95, 0 );
        wGcpRegion.setLayoutData( fdGcpRegion );
        lastControl = wGcpRegion;

        // Zone
        //
        Label wlGcpZone = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpZone );
        wlGcpZone.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpZone.Label" ) );
        FormData fdlGcpZone = new FormData();
        fdlGcpZone.top = new FormAttachment( lastControl, margin );
        fdlGcpZone.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpZone.right = new FormAttachment( middle, -margin );
        wlGcpZone.setLayoutData( fdlGcpZone );
        wGcpZone = new TextVar( transMeta, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wGcpZone );
        FormData fdGcpZone = new FormData();
        fdGcpZone.top = new FormAttachment( wlGcpZone, 0, SWT.CENTER );
        fdGcpZone.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpZone.right = new FormAttachment( 95, 0 );
        wGcpZone.setLayoutData( fdGcpZone );
        lastControl = wGcpZone;

        // Streaming?
        //
        Label wlGcpStreaming = new Label( wDataflowComp, SWT.RIGHT );
        props.setLook( wlGcpStreaming );
        wlGcpStreaming.setText( BaseMessages.getString( PKG, "BeamDataflowRunnerDialog.GcpStreaming.Label" ) );
        FormData fdlGcpStreaming = new FormData();
        fdlGcpStreaming.top = new FormAttachment( lastControl, margin );
        fdlGcpStreaming.left = new FormAttachment( 0, -margin ); // First one in the left top corner
        fdlGcpStreaming.right = new FormAttachment( middle, -margin );
        wlGcpStreaming.setLayoutData( fdlGcpStreaming );
        wGcpStreaming = new Button( wDataflowComp, SWT.CHECK | SWT.LEFT );
        props.setLook( wGcpStreaming );
        FormData fdGcpStreaming = new FormData();
        fdGcpStreaming.top = new FormAttachment( wlGcpStreaming, 0, SWT.CENTER );
        fdGcpStreaming.left = new FormAttachment( middle, 0 ); // To the right of the label
        fdGcpStreaming.right = new FormAttachment( 95, 0 );
        wGcpStreaming.setLayoutData( fdGcpStreaming );
        lastControl = wGcpStreaming;

        FormData fdDataflowComp = new FormData();
        fdDataflowComp.left = new FormAttachment( 0, 0 );
        fdDataflowComp.top = new FormAttachment( 0, 0 );
        fdDataflowComp.right = new FormAttachment( 100, 0 );
        fdDataflowComp.bottom = new FormAttachment( 100, 0 );
        wDataflowComp.setLayoutData( fdDataflowComp );

        wDataflowComp.pack();
        Rectangle bounds = wDataflowComp.getBounds();

        wDataflowSComp.setContent( wDataflowComp );
        wDataflowSComp.setExpandHorizontal( true );
        wDataflowSComp.setExpandVertical( true );
        wDataflowSComp.setMinWidth( bounds.width );
        wDataflowSComp.setMinHeight( bounds.height );

        wDataflowTab.setControl( wDataflowSComp );
    }

    public void getData() {
        wStepname.setText( stepname );
//        this.wName.setText( Const.NVL( this.meta.getName(), "" ) );

        // General
        wUserAgent.setText( Const.NVL( this.meta.getUserAgent(), "" ) );
        wTempLocation.setText( Const.NVL( this.meta.getTempLocation(), "" ) );
        wPluginsToStage.setText( Const.NVL( this.meta.getPluginsToStage(), "" ) );
        wStepPluginClasses.setText( Const.NVL( this.meta.getStepPluginClasses(), "" ) );
        wXpPluginClasses.setText( Const.NVL( this.meta.getXpPluginClasses(), "" ) );
        wFatJar.setText( Const.NVL( this.meta.getFatJar(), "" ) );
        wStreamingKettleStepsFlushInterval.setText( Const.NVL( this.meta.getStreamingKettleStepsFlushInterval(), "" ) );

        // GCP
        /*
         */
        wGcpProjectId.setText( Const.NVL( this.meta.getGcpProjectId(), "" ) );
        wGcpAppName.setText( Const.NVL( this.meta.getGcpAppName(), "" ) );
        wGcpStagingLocation.setText( Const.NVL( this.meta.getGcpStagingLocation(), "" ) );
        wGcpTemplateLocation.setText( Const.NVL( this.meta.getGcpTemplateLocation(), "" ) );
        wGcpNetwork.setText( Const.NVL( this.meta.getGcpNetwork(), "" ) );
        wGcpSubNetwork.setText( Const.NVL( this.meta.getGcpSubNetwork(), "" ) );

        String workerCode = this.meta.getGcpWorkerMachineType();
        String workerDescription = "";
        if ( StringUtils.isNotEmpty( workerCode ) ) {
            int index = Const.indexOfString( workerCode, BeamConst.getGcpWorkerMachineTypeCodes() );
            if ( index < 0 ) {
                workerDescription = workerCode; // variable, manually entered in general
            } else {
                workerDescription = BeamConst.getGcpWorkerMachineTypeDescriptions()[ index ];
            }
        }
        wGcpWorkerMachineType.setText( workerDescription );
        wGcpWorkerDiskType.setText( Const.NVL( this.meta.getGcpWorkerDiskType(), "" ) );
        wGcpDiskSizeGb.setText( Const.NVL( this.meta.getGcpDiskSizeGb(), "" ) );
        wGcpInitialNumberOfWorkers.setText( Const.NVL( this.meta.getGcpInitialNumberOfWorkers(), "" ) );
        wGcpMaximumNumberOfWorkers.setText( Const.NVL( this.meta.getGcpMaximumNumberOfWokers(), "" ) );
        wGcpAutoScalingAlgorithm.setText( Const.NVL( this.meta.getGcpAutoScalingAlgorithm(), "" ) );
        wGcpStreaming.setSelection( this.meta.isGcpStreaming() );
        String regionCode = this.meta.getGcpRegion();
        String regionDescription = "";
        if ( StringUtils.isNotEmpty( regionCode ) ) {
            int index = Const.indexOfString( regionCode, BeamConst.getGcpRegionCodes() );
            if ( index < 0 ) {
                regionDescription = regionCode; // variable, manually entered in general
            } else {
                regionDescription = BeamConst.getGcpRegionDescriptions()[ index ];
            }
        }
        wGcpRegion.setText( regionDescription );
        wGcpZone.setText( Const.NVL( this.meta.getGcpZone(), "" ) );

        // Parameters
        //
        int rowNr = 0;
        for ( RunnerParameter parameter : this.meta.getParameters() ) {
            TableItem item = this.wParameters.table.getItem( rowNr++ );
            item.setText( 1, Const.NVL( parameter.getVariable(), "" ) );
            item.setText( 2, Const.NVL( parameter.getValue(), "" ) );
        }
        wParameters.setRowNums();
        wParameters.optWidth( true );

        wStepname.selectAll();
        wStepname.setFocus();
    }

    private void cancel() {
        stepname = null;
        this.meta.setChanged( this.changed );
        dispose();
    }

    public void ok() {
        if (Utils.isEmpty(wStepname.getText())) {return;}
        getInfo( meta );
        dispose();
    }

    // Get dialog info in securityService
    private void getInfo( BeamDataflowRunnerMeta cfg ) {
        stepname = wStepname.getText();
        cfg.setName(stepname);
        cfg.setUserAgent( wUserAgent.getText() );
        cfg.setTempLocation( wTempLocation.getText() );
        cfg.setPluginsToStage( wPluginsToStage.getText() );
        cfg.setStepPluginClasses( ( wStepPluginClasses.getText() ) );
        cfg.setXpPluginClasses( ( wXpPluginClasses.getText() ) );
        cfg.setStreamingKettleStepsFlushInterval( wStreamingKettleStepsFlushInterval.getText() );
        cfg.setFatJar( wFatJar.getText() );
        cfg.setGcpProjectId( wGcpProjectId.getText() );
        cfg.setGcpAppName( wGcpAppName.getText() );
        cfg.setGcpStagingLocation( wGcpStagingLocation.getText() );
        cfg.setGcpTemplateLocation( wGcpTemplateLocation.getText() );
        cfg.setGcpNetwork( wGcpNetwork.getText() );
        cfg.setGcpSubNetwork( wGcpSubNetwork.getText() );
        cfg.setGcpInitialNumberOfWorkers( wGcpInitialNumberOfWorkers.getText() );
        cfg.setGcpMaximumNumberOfWokers( wGcpMaximumNumberOfWorkers.getText() );
        cfg.setGcpStreaming( wGcpStreaming.getSelection() );
        cfg.setGcpAutoScalingAlgorithm( wGcpAutoScalingAlgorithm.getText() );

        String workerMachineDesciption = wGcpWorkerMachineType.getText();
        String workerMachineCode = null;
        if ( StringUtils.isNotEmpty( workerMachineDesciption ) ) {
            int index = Const.indexOfString( workerMachineDesciption, BeamConst.getGcpWorkerMachineTypeDescriptions() );
            if ( index < 0 ) {
                workerMachineCode = workerMachineDesciption; // Variable or manually entered
            } else {
                workerMachineCode = BeamConst.getGcpWorkerMachineTypeCodes()[ index ];
            }
        }
        cfg.setGcpWorkerMachineType( workerMachineCode );

        cfg.setGcpWorkerDiskType( wGcpWorkerDiskType.getText() );
        cfg.setGcpDiskSizeGb( wGcpDiskSizeGb.getText() );
        cfg.setGcpZone( wGcpZone.getText() );

        String regionDesciption = wGcpRegion.getText();
        String regionCode = null;
        if ( StringUtils.isNotEmpty( regionDesciption ) ) {
            int index = Const.indexOfString( regionDesciption, BeamConst.getGcpRegionDescriptions() );
            if ( index < 0 ) {
                regionCode = regionDesciption; // Variable or manually entered
            } else {
                regionCode = BeamConst.getGcpRegionCodes()[ index ];
            }
        }
        cfg.setGcpRegion( regionCode );

        cfg.getParameters().clear();
        for ( int i = 0; i < wParameters.nrNonEmpty(); i++ ) {
            TableItem item = wParameters.getNonEmpty( i );
            cfg.getParameters().add( new RunnerParameter( item.getText( 1 ), item.getText( 2 ) ) );
        }

        cfg.setChanged();
    }
}
