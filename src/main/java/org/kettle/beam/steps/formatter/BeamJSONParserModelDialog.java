package org.kettle.beam.steps.formatter;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.util.Json;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.math.BigDecimal;
import java.util.*;
import java.util.List;

public class BeamJSONParserModelDialog extends Dialog {

    //region Attributes

    private static Class<?> PKG = BeamJSONParserModelDialog.class;
    private BeamJSONParserMeta input;

    private Shell shell;
    private PropsUI props;

    private int middle;
    private int margin;

    private Button wOK;
    private Button wCancel;
    private Text txtModel;

    private List<JSONField> fields;
    private BeamJSONParserDialog parserDialog;

    //endregion

    //region Getters Setters

    public List<JSONField> getFields(){
        if(this.fields == null){this.fields = new ArrayList<>();}
        return this.fields;
    }

    //endregion


    //region Constructors

    /**
     * Construtor padrão
     *
     * @param parserDialog
     */
    public BeamJSONParserModelDialog(BeamJSONParserDialog parserDialog) {
        super(parserDialog.getParent());
        this.parserDialog = parserDialog;
    }

    //endregion

    //region Methods

    /**
     * Método para abrir a tela.
     *
     * @return
     */
    public String open() {

        Shell parent = this.getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        this.props = PropsUI.getInstance();
        this.props.setLook(this.shell);

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        FormData formData;

        this.shell.setLayout( formLayout );
        this.shell.setText( BaseMessages.getString( PKG, "BeamJSONParserModelDialog.DialogTitle" ) );

        this.middle = this.props.getMiddlePct();
        this.margin = Const.MARGIN;


        this.wOK = new Button( this.shell, SWT.PUSH );
        this.wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        this.wCancel = new Button( this.shell, SWT.PUSH );
        this.wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
        Button[] buttons = new Button[]{this.wOK, this.wCancel};
        BaseStepDialog.positionBottomButtons(this.shell, buttons, margin, (Control)null);

        txtModel = new Text( shell, SWT.LEFT | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL  );
        props.setLook( txtModel , Props.WIDGET_STYLE_FIXED);
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( 0, margin );
        formData.right = new FormAttachment( 100, -margin );
        formData.bottom = new FormAttachment( wOK, -2*margin);
        txtModel.setLayoutData( formData );
        txtModel.setBackground(new Color(Display.getCurrent(),255, 250,245));

        this.wOK.addListener( SWT.Selection, e -> this.ok() );
        this.wCancel.addListener( SWT.Selection, e -> this.cancel() );


        // Detect X or ALT-F4 or something that kills this window...
        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                BeamJSONParserModelDialog.this.cancel();
            }
        });

        BaseStepDialog.setSize(this.shell);

        this.shell.open();
        while ( !this.shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }

        return "OK";

    }


    private void cancel() {
        dispose();
    }

    private void ok() {
        try {
            this.getFields().clear();
            String json = txtModel.getText().trim();
            if(!Strings.isNullOrEmpty(json)) {
                if(json.startsWith("[")){
                    this.parseJsonArray(json);
                }else if(json.startsWith("{")){
                    this.parseJsonObject(json);
                }
            }
            parserDialog.setFields(this.getFields());
            dispose();
        }catch (Exception ex){
            SimpleMessageDialog.openError(this.shell, "Error", ex.getMessage());
        }
    }

    private void parseJsonObject(String json) throws Exception {
        Map<String, Object> jsonObject = Json.getInstance().deserialize(json);
        this.parseJson("", jsonObject);
    }

    private void parseJsonArray(String json) throws Exception {
        List<Map<String, Object>> list = Json.getInstance().deserializeList(json);
        if(list.size() > 0){
            this.parseJson("", list.get(0));
        }
    }

    private void parseJson(String prefix, Map<String, Object> jsonObject){
        Object value;
        String attributeName;
        JSONField field;
        ValueMetaInterface valueMeta;

        for(Map.Entry<String, Object> entry: jsonObject.entrySet()){
            value = entry.getValue();
            attributeName = prefix + entry.getKey().trim();

            if (value != null && value instanceof Map) {
                this.parseJson(attributeName + "_", (Map<String, Object>) value);

            } else if (value != null && value instanceof List) {
                this.getFields().add(new JSONField(attributeName + "_count", "", JSONField.BEAM_DATATYPE_INTEGER));
                List<Map<String, Object>> list = (List<Map<String, Object>>) value;
                int i = 0;
                for (Map<String, Object> jsonObjectChild : list) {
                    this.parseJson(attributeName + "_" + i + "_", (Map<String, Object>) jsonObjectChild);
                    i++;
                }

            } else {
                this.getFields().add(new JSONField(attributeName, "", this.getKettleType(value)));

            }

        }
    }


    public void dispose() {
        try {
            WindowProperty winprop = new WindowProperty(this.shell);
            this.props.setScreen(winprop);
            this.shell.dispose();
        }catch (Exception ex){}
    }




    private String getKettleType(Object value){
        if(value == null){
            return JSONField.BEAM_DATATYPE_STRING;

        }else if(value instanceof java.sql.Date
                || value instanceof java.util.Date){
            return JSONField.BEAM_DATATYPE_DATE;

        }else if(value instanceof Calendar) {
            return JSONField.BEAM_DATATYPE_TIMESTAMP;

        }else if(value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return JSONField.BEAM_DATATYPE_INTEGER;

        }else if(value instanceof BigDecimal
                || value instanceof Double
                || value instanceof Float) {
            return JSONField.BEAM_DATATYPE_BIG_NUMBER;

        }else if(value instanceof Boolean) {
            return JSONField.BEAM_DATATYPE_BOOLEAN;

        }else{
            return JSONField.BEAM_DATATYPE_STRING;

        }

    }


    //endregion

}
