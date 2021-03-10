package org.kettle.beam.job.gcp;

import org.pentaho.di.core.Result;

public class WorkflowFinisher {

    private WorkflowFinisherMeta meta;

    public WorkflowFinisher(WorkflowFinisherMeta meta){
        this.meta = meta;
    }

    public Result execute(Result prevResult, int nr ){
        prevResult.setNrErrors( 0 );
        prevResult.setResult(true);
        return prevResult;
    }

}
