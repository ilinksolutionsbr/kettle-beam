package org.kettle.beam.job.dataflow;

import org.pentaho.di.core.Result;

public class DataFlowRunner {

    private DataFlowRunnerMeta meta;

    public DataFlowRunner(DataFlowRunnerMeta meta){
        this.meta = meta;
    }

    public Result execute(Result prevResult, int nr ){
        prevResult.setNrErrors( 0 );
        prevResult.setResult(true);
        return prevResult;
    }

}
