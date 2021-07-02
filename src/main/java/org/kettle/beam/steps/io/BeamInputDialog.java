
package org.kettle.beam.steps.io;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.core.util.Web;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.bq.BQField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class BeamInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamInput.class; // for i18n purposes, needed by Translator2!!
  private final BeamInputMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private TextVar wInputLocation;

  Control lastControl;

  private Text wSeparator;
  private Text wEnclosure;
  private TableView wFields;

  private Button wLoadFileDefinition;
  private Listener lsFileDefinition;

  public BeamInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamInputMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    wLoadFileDefinition = new Button( shell, SWT.PUSH );
    wLoadFileDefinition.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Metastore.Load" ) );

    setButtonPositions( new Button[] { wOK, wCancel, wLoadFileDefinition }, margin, null );

    // The rest of the dialog is for the widgets...
    //
    addFormWidgets();

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

    lsFileDefinition = new Listener() {
      public void handleEvent( Event e ) {
        loadFileDefinition();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );
    wLoadFileDefinition.addListener( SWT.Selection, lsFileDefinition );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wInputLocation.addSelectionListener( lsDef );
    wSeparator.addSelectionListener( lsDef );
    wEnclosure.addSelectionListener( lsDef );

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

  private void addFormWidgets() {

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
    lastControl = wStepname;

    Label wlInputLocation = new Label( shell, SWT.RIGHT );
    wlInputLocation.setText( BaseMessages.getString( PKG, "BeamInputDialog.InputLocation" ) );
    props.setLook( wlInputLocation );
    FormData fdlInputLocation = new FormData();
    fdlInputLocation.left = new FormAttachment( 0, 0 );
    fdlInputLocation.top = new FormAttachment( lastControl, margin );
    fdlInputLocation.right = new FormAttachment( middle, -margin );
    wlInputLocation.setLayoutData( fdlInputLocation );
    wInputLocation = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputLocation );
    FormData fdInputLocation = new FormData();
    fdInputLocation.left = new FormAttachment( middle, 0 );
    fdInputLocation.top = new FormAttachment( wlInputLocation, 0, SWT.CENTER );
    fdInputLocation.right = new FormAttachment( 100, 0 );
    wInputLocation.setLayoutData( fdInputLocation );
    lastControl = wInputLocation;

    // Separator
    //
    Label wlSeparator = new Label( shell, SWT.RIGHT );
    props.setLook( wlSeparator );
    wlSeparator.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Separator.Label" ) );
    FormData fdlSeparator = new FormData();
    fdlSeparator.top = new FormAttachment( lastControl, 16 );
    fdlSeparator.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlSeparator.right = new FormAttachment( middle, -margin );
    wlSeparator.setLayoutData( fdlSeparator );
    wSeparator = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeparator );
    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment( wlSeparator, 0, SWT.CENTER );
    fdSeparator.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSeparator.right = new FormAttachment( 100, 0 );
    wSeparator.setLayoutData( fdSeparator );
    lastControl = wSeparator;

    // Enclosure
    //
    Label wlEnclosure = new Label( shell, SWT.RIGHT );
    props.setLook( wlEnclosure );
    wlEnclosure.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Enclosure.Label" ) );
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.top = new FormAttachment( lastControl, margin );
    fdlEnclosure.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlEnclosure.right = new FormAttachment( middle, -margin );
    wlEnclosure.setLayoutData( fdlEnclosure );
    wEnclosure = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosure );
    FormData fdEnclosure = new FormData();
    fdEnclosure.top = new FormAttachment( wlEnclosure, 0, SWT.CENTER );
    fdEnclosure.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdEnclosure.right = new FormAttachment( 100, 0 );
    wEnclosure.setLayoutData( fdEnclosure );
    lastControl = wEnclosure;

    // Fields...
    //
    Label wlFields = new Label( shell, SWT.LEFT );
    props.setLook( wlFields );
    wlFields.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Label" ) );
    FormData fdlFields = new FormData();
    fdlFields.top = new FormAttachment( lastControl, margin );
    fdlFields.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlFields.right = new FormAttachment( 100, 0);
    wlFields.setLayoutData( fdlFields );

    ColumnInfo[] columnInfos = new ColumnInfo[] {
            new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldName" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
            new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), false ),
            new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldFormat" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
            new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldLength" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
            new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldPrecision" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };

    wFields = new TableView( new Variables(), shell, SWT.BORDER, columnInfos, input.getFields().size(), null, props );
    props.setLook( wFields );
    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdFields.right = new FormAttachment( 100, 0);
    fdFields.bottom = new FormAttachment( wOK, -margin*2);
    wFields.setLayoutData( fdFields );

  }

  private void loadFileDefinition() {
    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( shell, names,
              BaseMessages.getString( PKG, "FileDefinitionDialog.SelectDefinitionToEdit.Title" ),
              BaseMessages.getString( PKG, "FileDefinitionDialog.SelectDefinitionToEdit.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        FileDefinition fileDefinition = factory.loadElement( choice );

        wSeparator.setText(Const.NVL(fileDefinition.getSeparator(), ""));
        wEnclosure.setText(Const.NVL(fileDefinition.getEnclosure(), ""));

        wFields.clearAll();
        for(int i = 0; i < fileDefinition.getFieldDefinitions().size(); i++) {
          FieldDefinition field = fileDefinition.getFieldDefinitions().get( i );
          TableItem item = new TableItem(wFields.table, SWT.NONE);
          item.setText(1, Const.NVL(field.getName(), ""));
          item.setText(2, Const.NVL(field.getKettleType(), ""));
          item.setText(3, Const.NVL(field.getFormatMask(), ""));
          item.setText(4, field.getLength()<0 ? "" : Integer.toString(field.getLength()));
          item.setText(5, field.getPrecision()<0 ? "" : Integer.toString(field.getPrecision()));
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }

    } catch (MetaStoreException e) {

    }
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
    helpButton.setText(BaseMessages.getString( PKG, "BeamInputDialog.HelpButton" ));
    helpButton.addListener(SWT.Selection, e ->
            Web.open(this, BaseMessages.getString( PKG, "BeamInputDialog.HelpLink" ))
    );
    return helpButton;
  }

  /**
   * Populate the widgets.
   */
  public void getData( ) {
    wStepname.setText( stepname );
    wInputLocation.setText(Const.NVL(input.getInputLocation(), ""));
    wSeparator.setText(Const.NVL(input.getSeparator(), ""));
    wEnclosure.setText(Const.NVL(input.getEnclosure(), ""));

    for (int i=0;i<input.getFields().size();i++) {
      FieldDefinition field = input.getFields().get( i );
      TableItem item = wFields.table.getItem( i );
      item.setText( 1, Const.NVL(field.getName(), "") );
      item.setText( 2, Const.NVL(field.getKettleType(), "") );
      item.setText( 3, Const.NVL(field.getFormatMask(), "") );
      item.setText(4, field.getLength()<0 ? "" : Integer.toString(field.getLength()));
      item.setText(5, field.getPrecision()<0 ? "" : Integer.toString(field.getPrecision()));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    try {
      if (Utils.isEmpty(wStepname.getText())) {return;}
      if (Strings.isNullOrEmpty(wInputLocation.getText()) ) {throw new Exception("Arquivo de entrada nao informado.");}
      getInfo(input);
      dispose();

    }catch (Exception ex){
      SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

    }
  }

  private void getInfo( BeamInputMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setInputLocation( wInputLocation.getText() );
    in.setSeparator( wSeparator.getText() );
    in.setEnclosure( wEnclosure.getText() );
    in.getFields().clear();
    for (int i=0;i<wFields.nrNonEmpty();i++) {
      TableItem item = wFields.getNonEmpty( i );
      String name = item.getText(1);
      String kettleType = item.getText(2);
      String formatMask = item.getText(3);
      int length = Const.toInt(item.getText(4), -1);
      int precision = Const.toInt(item.getText(5), -1);
      in.getFields().add(new FieldDefinition( name, kettleType, length, precision, formatMask ));
    }

    input.setChanged();
  }
}