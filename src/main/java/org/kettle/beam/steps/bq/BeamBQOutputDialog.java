
package org.kettle.beam.steps.bq;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.core.util.Web;
import org.kettle.beam.steps.database.FieldInfo;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.awt.*;


public class BeamBQOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamBQOutputDialog.class; // for i18n purposes, needed by Translator2!!
  private final BeamBQOutputMeta input;

  int middle;
  int margin;

  private TextVar wProjectId;
  private TextVar wDatasetId;
  private TextVar wTableId;
  private TextVar wTempLocation;
  private Button wCreateIfNeeded;
  private Button wTruncateTable;
  private Button wFailIfNotEmpty;

  private CTabFolder wTabFolder;
  private CTabItem wFieldsTab;
  private CTabItem wQueryTab;

  private ScrolledComposite wFieldsSComp;
  private ScrolledComposite wQuerySComp;

  private Composite wFieldsComp;
  private Composite wQueryComp;

  private Text wQuery;
  private TableView tblFields;


  public BeamBQOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamBQOutputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

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

    Label wlProjectId = new Label( shell, SWT.RIGHT );
    wlProjectId.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.ProjectId" ) );
    props.setLook( wlProjectId );
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment( 0, 0 );
    fdlProjectId.top = new FormAttachment( lastControl, margin );
    fdlProjectId.right = new FormAttachment( middle, -margin );
    wlProjectId.setLayoutData( fdlProjectId );
    wProjectId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wProjectId );
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment( middle, 0 );
    fdProjectId.top = new FormAttachment( wlProjectId, 0, SWT.CENTER );
    fdProjectId.right = new FormAttachment( 100, 0 );
    wProjectId.setLayoutData( fdProjectId );
    lastControl = wProjectId;

    Label wlDatasetId = new Label( shell, SWT.RIGHT );
    wlDatasetId.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.DatasetId" ) );
    props.setLook( wlDatasetId );
    FormData fdlDatasetId = new FormData();
    fdlDatasetId.left = new FormAttachment( 0, 0 );
    fdlDatasetId.top = new FormAttachment( lastControl, margin );
    fdlDatasetId.right = new FormAttachment( middle, -margin );
    wlDatasetId.setLayoutData( fdlDatasetId );
    wDatasetId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDatasetId );
    FormData fdDatasetId = new FormData();
    fdDatasetId.left = new FormAttachment( middle, 0 );
    fdDatasetId.top = new FormAttachment( wlDatasetId, 0, SWT.CENTER );
    fdDatasetId.right = new FormAttachment( 100, 0 );
    wDatasetId.setLayoutData( fdDatasetId );
    lastControl = wDatasetId;

    Label wlTableId = new Label( shell, SWT.RIGHT );
    wlTableId.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.TableId" ) );
    props.setLook( wlTableId );
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment( 0, 0 );
    fdlTableId.top = new FormAttachment( lastControl, margin );
    fdlTableId.right = new FormAttachment( middle, -margin );
    wlTableId.setLayoutData( fdlTableId );
    wTableId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTableId );
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment( middle, 0 );
    fdTableId.top = new FormAttachment( wlTableId, 0, SWT.CENTER );
    fdTableId.right = new FormAttachment( 100, 0 );
    wTableId.setLayoutData( fdTableId );
    lastControl = wTableId;

    Label wlTempLocation = new Label( shell, SWT.RIGHT );
    wlTempLocation.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.TempLocation" ) );
    props.setLook( wlTempLocation );
    FormData fdlTempLocation = new FormData();
    fdlTempLocation.left = new FormAttachment( 0, 0 );
    fdlTempLocation.top = new FormAttachment( lastControl, margin );
    fdlTempLocation.right = new FormAttachment( middle, -margin );
    wlTempLocation.setLayoutData( fdlTempLocation );
    wTempLocation = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTempLocation );
    FormData fdTempLocation = new FormData();
    fdTempLocation.left = new FormAttachment( middle, 0 );
    fdTempLocation.top = new FormAttachment( wlTempLocation, 0, SWT.CENTER );
    fdTempLocation.right = new FormAttachment( 100, 0 );
    wTempLocation.setLayoutData( fdTempLocation );
    lastControl = wTempLocation;

    Label wlCreateIfNeeded = new Label( shell, SWT.RIGHT );
    wlCreateIfNeeded.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.CreateIfNeeded" ) );
    props.setLook( wlCreateIfNeeded );
    FormData fdlCreateIfNeeded = new FormData();
    fdlCreateIfNeeded.left = new FormAttachment( 0, 0 );
    fdlCreateIfNeeded.top = new FormAttachment( lastControl, margin );
    fdlCreateIfNeeded.right = new FormAttachment( middle, -margin );
    wlCreateIfNeeded.setLayoutData( fdlCreateIfNeeded );
    wCreateIfNeeded = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wCreateIfNeeded );
    FormData fdCreateIfNeeded = new FormData();
    fdCreateIfNeeded.left = new FormAttachment( middle, 0 );
    fdCreateIfNeeded.top = new FormAttachment( wlCreateIfNeeded, 0, SWT.CENTER );
    fdCreateIfNeeded.right = new FormAttachment( 100, 0 );
    wCreateIfNeeded.setLayoutData( fdCreateIfNeeded );
    lastControl = wCreateIfNeeded;

    Label wlTruncateTable = new Label( shell, SWT.RIGHT );
    wlTruncateTable.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.TruncateTable" ) );
    props.setLook( wlTruncateTable );
    FormData fdlTruncateTable = new FormData();
    fdlTruncateTable.left = new FormAttachment( 0, 0 );
    fdlTruncateTable.top = new FormAttachment( lastControl, margin );
    fdlTruncateTable.right = new FormAttachment( middle, -margin );
    wlTruncateTable.setLayoutData( fdlTruncateTable );
    wTruncateTable = new Button( shell,  SWT.CHECK | SWT.LEFT );
    props.setLook( wTruncateTable );
    FormData fdTruncateTable = new FormData();
    fdTruncateTable.left = new FormAttachment( middle, 0 );
    fdTruncateTable.top = new FormAttachment( wlTruncateTable, 0, SWT.CENTER );
    fdTruncateTable.right = new FormAttachment( 100, 0 );
    wTruncateTable.setLayoutData( fdTruncateTable );
    lastControl = wTruncateTable;

    Label wlFailIfNotEmpty = new Label( shell, SWT.RIGHT );
    wlFailIfNotEmpty.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.FailIfNotEmpty" ) );
    props.setLook( wlFailIfNotEmpty );
    FormData fdlFailIfNotEmpty = new FormData();
    fdlFailIfNotEmpty.left = new FormAttachment( 0, 0 );
    fdlFailIfNotEmpty.top = new FormAttachment( lastControl, margin );
    fdlFailIfNotEmpty.right = new FormAttachment( middle, -margin );
    wlFailIfNotEmpty.setLayoutData( fdlFailIfNotEmpty );
    wFailIfNotEmpty = new Button( shell,  SWT.CHECK | SWT.LEFT );
    props.setLook( wFailIfNotEmpty );
    FormData fdFailIfNotEmpty = new FormData();
    fdFailIfNotEmpty.left = new FormAttachment( middle, 0 );
    fdFailIfNotEmpty.top = new FormAttachment( wlFailIfNotEmpty, 0, SWT.CENTER );
    fdFailIfNotEmpty.right = new FormAttachment( 100, 0 );
    wFailIfNotEmpty.setLayoutData( fdFailIfNotEmpty );
    lastControl = wFailIfNotEmpty;


    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    this.setButtonPositions( new Button[] { this.wOK, this.wCancel }, this.margin, null );


    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( lastControl, margin);
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOK, -2*margin);
    wTabFolder.setLayoutData( fdTabFolder );

    addFieldsTab();
    addQueryTab();

    wTabFolder.setSelection( 0 );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wProjectId.addSelectionListener( lsDef );
    wDatasetId.addSelectionListener( lsDef );
    wTableId.addSelectionListener( lsDef );
    wQuery.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( );
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }


  private void addFieldsTab(){
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( "  Campos  " );

    wFieldsSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wFieldsSComp.setLayout( new FillLayout() );

    wFieldsComp = new Composite( wFieldsSComp, SWT.COLOR_WIDGET_BACKGROUND );
    props.setLook( wFieldsComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 0;
    generalLayout.marginHeight = 0;
    wFieldsComp.setLayout( generalLayout );

    String[] fieldNames;
    String[] values = new String[]{"Sim", "Não"};
    try {
      fieldNames = this.transMeta.getPrevStepFields(this.stepMeta).getFieldNames();
    } catch( KettleException e ) {
      log.logError("Error getting fields from previous steps", e);
      fieldNames = new String[] {};
    }

    ColumnInfo fieldColumn = new ColumnInfo( BaseMessages.getString( PKG, "BeamBQOutputDialog.Column.Field" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    ColumnInfo selectColumn = new ColumnInfo( BaseMessages.getString( PKG, "BeamBQOutputDialog.Column.Select" ), ColumnInfo.COLUMN_TYPE_CCOMBO, values, false );
    ColumnInfo[] columns = new ColumnInfo[] {fieldColumn, selectColumn,};

    this.tblFields = new TableView( Variables.getADefaultVariableSpace(), wFieldsComp, SWT.NONE, columns, fieldNames != null ? fieldNames.length : 0, null, props);
    props.setLook( this.tblFields );
    FormData formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.bottom = new FormAttachment( 100, 0);
    this.tblFields.setLayoutData(formData);
    this.tblFields.addModifyListener((e) -> {this.refreshTable();});

    if(fieldNames != null) {
      String fieldName;
      for (int i = 0; i < fieldNames.length; i++) {
        fieldName = fieldNames[i];
        TableItem item = this.tblFields.table.getItem(i);
        item.setText(1, fieldName);
        item.setText(2, "Sim");
      }
      this.tblFields.removeEmptyRows();
      this.tblFields.setRowNums();
      this.tblFields.optWidth(true);
      this.refreshTable();
    }

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.pack();
    Rectangle bounds = wFieldsComp.getBounds();

    wFieldsSComp.setContent( wFieldsComp );
    wFieldsSComp.setExpandHorizontal( true );
    wFieldsSComp.setExpandVertical( true );
    wFieldsSComp.setMinWidth( bounds.width );
    wFieldsSComp.setMinHeight( bounds.height );

    wFieldsTab.setControl( wFieldsSComp );
  }


  private void refreshTable(){
    for (int i = 0; i < tblFields.nrNonEmpty(); i++) {
      TableItem item = tblFields.getNonEmpty(i);
      if("Sim".equalsIgnoreCase(item.getText(2))){
        item.setBackground(new Color(Display.getCurrent(),240,254,240));
      }else{
        item.setBackground(new Color(Display.getCurrent(),255, 240,244));
      }
    }
    this.tblFields.redraw();
  }

  private void addQueryTab(){
    wQueryTab = new CTabItem( wTabFolder, SWT.NONE );
    wQueryTab.setText( "  Query  " );

    wQuerySComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wQuerySComp.setLayout( new FillLayout() );

    wQueryComp = new Composite( wQuerySComp, SWT.COLOR_WIDGET_BACKGROUND );
    props.setLook( wQueryComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 0;
    generalLayout.marginHeight = 0;
    wQueryComp.setLayout( generalLayout );
    wQueryComp.setBackground(new Color(Display.getCurrent(),255, 250,245));

    wQuery = new Text( wQueryComp, SWT.LEFT | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wQuery, Props.WIDGET_STYLE_FIXED);
    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment( 0, 0 );
    fdQuery.top = new FormAttachment( 0, 0 );
    fdQuery.right = new FormAttachment( 100, 0 );
    fdQuery.bottom = new FormAttachment( 100, 0);
    wQuery.setLayoutData( fdQuery );
    wQuery.setBackground(new Color(Display.getCurrent(),255, 250,245));

    FormData fdQueryComp = new FormData();
    fdQueryComp.left = new FormAttachment( 0, 0 );
    fdQueryComp.top = new FormAttachment( 0, 0 );
    fdQueryComp.right = new FormAttachment( 100, 0 );
    fdQueryComp.bottom = new FormAttachment( 100, 0 );
    wQueryComp.setLayoutData( fdQueryComp );

    wQueryComp.pack();
    Rectangle bounds = wQueryComp.getBounds();

    wQuerySComp.setContent( wQueryComp );
    wQuerySComp.setExpandHorizontal( true );
    wQuerySComp.setExpandVertical( true );
    wQuerySComp.setMinWidth( bounds.width );
    wQuerySComp.setMinHeight( bounds.height );

    wQueryTab.setControl( wQuerySComp );
  }


  /**
   * Método responsável por criar e o botão de ajuda que quando clicado
   * abre uma pagína de ajuda para o componente.
   *
   * @param shell
   * @param stepMeta
   * @param plugin
   * @return
   */
  @Override
  protected Button createHelpButton(Shell shell, StepMeta stepMeta, PluginInterface plugin) {
    Button helpButton = new Button(shell, SWT.PUSH);
    helpButton.setText(BaseMessages.getString( PKG, "BeamBQOutputDialog.HelpButton" ));
    helpButton.addListener(SWT.Selection, e ->
            Web.open(this, BaseMessages.getString( PKG, "BeamBQOutputDialog.HelpLink" ))
    );
    return helpButton;
  }

  /**
   * Populate the widgets.
   */
  public void getData( ) {
    wStepname.setText( stepname );
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wDatasetId.setText(Const.NVL(input.getDatasetId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wTempLocation.setText(Const.NVL(input.getTempLocation(), ""));
    wCreateIfNeeded.setSelection( input.isCreatingIfNeeded() );
    wTruncateTable.setSelection( input.isTruncatingTable() );
    wFailIfNotEmpty.setSelection( input.isFailingIfNotEmpty() );
    wQuery.setText( Const.NVL(input.getQuery(), "") );
    wStepname.selectAll();
    wStepname.setFocus();

    if(input.getFields().size() > 0) {
      String fieldName;
      for (int i = 0; i < tblFields.nrNonEmpty(); i++) {
        TableItem item = tblFields.getNonEmpty(i);
        fieldName = item.getText(1).trim();
        item.setText(2, input.getFields().contains(fieldName) ? "Sim" : "Não");
      }
    }

    this.refreshTable();

  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    try {
      if (Utils.isEmpty(wStepname.getText())) {return;}
      if (Strings.isNullOrEmpty(wProjectId.getText()) ) {throw new Exception("Projeto nao informado.");}
      if (Strings.isNullOrEmpty(wDatasetId.getText()) ) {throw new Exception("DataSet nao informado.");}
      if (Strings.isNullOrEmpty(wTableId.getText()) ) {throw new Exception("Tabela nao informada.");}
      getInfo(input);
      dispose();

    }catch (Exception ex){
      SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

    }
  }

  private void getInfo( BeamBQOutputMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setProjectId( wProjectId.getText() );
    in.setDatasetId( wDatasetId.getText() );
    in.setTableId( wTableId.getText() );
    in.setTempLocation( wTempLocation.getText() );
    in.setCreatingIfNeeded( wCreateIfNeeded.getSelection() );
    in.setTruncatingTable( wTruncateTable.getSelection() );
    in.setFailingIfNotEmpty( wFailIfNotEmpty.getSelection() );
    in.setQuery( wQuery.getText() );

    String fieldName;
    in.getFields().clear();
    for (int i = 0; i < tblFields.nrNonEmpty(); i++) {
      TableItem item = tblFields.getNonEmpty(i);
      fieldName = item.getText(1).trim();
      if("Sim".equalsIgnoreCase(item.getText(2).trim())) {
        in.getFields().add(fieldName);
      }
    }

    input.setChanged();
  }
}
