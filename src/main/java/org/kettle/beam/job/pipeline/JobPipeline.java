package org.kettle.beam.job.pipeline;

import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;

import java.util.ArrayList;
import java.util.List;

public class JobPipeline {

    //region Attributes

    private JobMeta jobMeta;

    //endregion

    //region Getters Setters

    public JobMeta getJobMeta(){
        return jobMeta;
    }

    //endregion

    //region Constructors

    public JobPipeline(JobMeta jobMeta){
        this.jobMeta = jobMeta;
    }

    //endregion

    //region Methods

    public String toGcpYAML(){
        IJobPipelineRender render = new JobGcpYAMLRender(this);
        return render.render();
    }

    //endregion

}
