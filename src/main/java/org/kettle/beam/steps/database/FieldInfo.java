package org.kettle.beam.steps.database;

import org.kettle.beam.core.util.Strings;

import java.io.Serializable;

public class FieldInfo implements Serializable {

    //region Attributes

    private String column;
    private String variable;
    private String type;

    //endregion

    //region Getters Setters


    public String getName() {
        if (!Strings.isNullOrEmpty(this.variable)) {
            return this.variable;
        } else {
            return this.column;
        }
    }

    public String getColumn() {
        return this.column;
    }
    public void setColumn( String value ) {
        this.column = value;
    }

    public String getVariable() {
        return this.variable;
    }
    public void setVariable( String value ) {
        this.variable = value;
    }

    public String getType() {
        return this.type;
    }
    public void setType( String value ) {
        this.type = value;
    }

    //endregion


    //region Constructors

    public FieldInfo() {}
    public FieldInfo(String column, String variable, String type ) {
        this.column = column;
        this.variable = variable;
        this.type = type;
    }

    //endregion

}
