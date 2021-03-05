package org.kettle.beam.job.pipeline;

import org.kettle.beam.job.gcp.DataflowRunnerMeta;
import org.kettle.beam.job.gcp.WorkflowFinisherMeta;
import org.pentaho.di.job.JobHopMeta;

import java.util.*;

public class JobGcpYAMLRender implements  IJobPipelineRender {

    //region Attributes

    private JobPipeline pipeline;
    private static final String FUNCTION_INVOKE_NAME = "functionInvoke";
    private static final String FUNCTION_WAIT_NAME = "functionWait";
    private static final String FUNCTION_WAIT_SECOND = "60";

    //endregion

    //region Constructors

    public JobGcpYAMLRender(JobPipeline pipeline){
        this.pipeline = pipeline;
    }

    //endregion

    //region Methods


    @Override
    public String render() {
        JobEntryNode rootNode = this.getRootNode();
        if(rootNode == null){return null;}
        StringBuilder builder = new StringBuilder();

        this.appendLine(builder, 0, "main:");
        this.appendLine(builder, 1, "steps:");
        this.renderGcpMainStepYAML(builder, 2, rootNode);
        this.renderGcpInvokeFunctionYAML(builder, 0);
        this.renderGcpWaitFunctionYAML(builder, 0);

        return builder.toString();
    }

    private JobEntryNode getRootNode(){
        Map<String, JobEntryNode> nodes = new HashMap<>();
        JobEntryNode rootNode = null;
        JobEntryNode node1;
        JobEntryNode node2;
        for(JobHopMeta hopMeta : this.pipeline.getJobMeta().getJobhops()){
            node1 = nodes.get(hopMeta.getFromEntry().getName());
            if(node1 == null){
                node1 = new JobEntryNode(hopMeta.getFromEntry().getName(), hopMeta.getFromEntry().getEntry());
                nodes.put(node1.getName(), node1);
            }

            node2 = nodes.get(hopMeta.getToEntry().getName());
            if(node2 == null){
                node2 = new JobEntryNode(hopMeta.getToEntry().getName(), hopMeta.getToEntry().getEntry());
                nodes.put(node2.getName(), node2);
            }

            node1.setNext(node2);
            node2.setPrevious(node1);
        }
        for(Map.Entry<String, JobEntryNode> entry : nodes.entrySet()){
            if(entry.getValue().getPrevious() == null){
                rootNode = entry.getValue();
                break;
            }
        }
        return rootNode;
    }

    private void renderGcpMainStepYAML(StringBuilder builder, int indent, JobEntryNode node){
        if(node != null) {
            if(node.getEntry() instanceof DataflowRunnerMeta){
                this.renderGcpDataFlowRunnerYAML(builder, indent, node);

            }else if(node.getEntry() instanceof WorkflowFinisherMeta){
                this.renderGcpFinisherYAML(builder, indent, node);
            }

            this.renderGcpMainStepYAML(builder, indent, node.getNext());

        }else{
            this.renderGcpReturnErrorYAML(builder, indent);

        }
    }

    private void appendLine(StringBuilder builder, int indent, String value){
        for(int i = 0; i < indent; i++){
            builder.append("  ");
        }
        builder.append(value + "\n");
    }

    private String getName(JobEntryNode node){
        if(node == null){return "null";}
        String name = node.getName().replaceAll(" ","").replaceAll("-","").replaceAll("_","");
        name = name.substring(0,1).toUpperCase() + name.substring(1);
        return name;
    }

    private String getValue(String value){
        return this.pipeline.getJobMeta().environmentSubstitute(value);
    }



    private void renderGcpDataFlowRunnerYAML(StringBuilder builder, int indent, JobEntryNode node) {
        if(node.getNext() == null){return;}
        DataflowRunnerMeta meta = (DataflowRunnerMeta)node.getEntry();

        String name = this.getName(node);

        String dataflowUrl = "https://dataflow.googleapis.com/v1b3/projects/" + this.getValue(meta.getProjectId()) + "/locations/" + this.getValue(meta.getRegion()) + "/templates:launch?gcsPath=" + this.getValue(meta.getTemplateLocation());

        String runName = "run" + name;
        String resultRunName = "resultRun" + name;
        String waitName = "wait" + name;
        String resultWaitName = "resultWait" + name;
        String switchName = "switch" + name;

        String runNextName = !(node.getNext() != null && node.getNext().getEntry() instanceof WorkflowFinisherMeta)
                ? "run" + this.getName(node.getNext())
                : "return" + this.getName(node.getNext());

        //Build RUN
        this.appendLine(builder, indent, "- " + runName + ":");
        this.appendLine(builder, indent + 2, "call: " + FUNCTION_INVOKE_NAME);
        this.appendLine(builder, indent + 2, "args:");
        this.appendLine(builder, indent + 3, "url: \"" + dataflowUrl + "\"");
        this.appendLine(builder, indent + 3, "jobName: \"" + this.getValue(meta.getJobName()) + "\"");
        this.appendLine(builder, indent + 3, "tempLocation: \"" + this.getValue(meta.getTempLocation()) + "\"");
        this.appendLine(builder, indent + 3, "zone: \"" + this.getValue(meta.getZone()) + "\"");
        this.appendLine(builder, indent + 2, "result: " + resultRunName);

        //Build WAIT
        this.appendLine(builder, indent, "- " + waitName + ":");
        this.appendLine(builder, indent + 2, "call: " + FUNCTION_WAIT_NAME);
        this.appendLine(builder, indent + 2, "args:");
        this.appendLine(builder, indent + 3, "execution: ${" + resultRunName + "}");
        this.appendLine(builder, indent + 3, "project: \"" + this.getValue(meta.getProjectId()) + "\"");
        this.appendLine(builder, indent + 2, "result: " + resultWaitName);
        this.appendLine(builder, indent + 2, "next: " + switchName);

        //Build SWITCH
        this.appendLine(builder, indent, "- " + switchName + ":");
        this.appendLine(builder, indent + 2, "switch:");
        this.appendLine(builder, indent + 4, "- condition: ${ " + resultWaitName + " == \"Ok\"}");
        this.appendLine(builder, indent + 5, "next: " + runNextName);
        this.appendLine(builder, indent + 4, "- condition: ${ not(" + resultWaitName + " == \"Ok\") }");
        this.appendLine(builder, indent + 5, "assign:");
        this.appendLine(builder, indent + 6, "- errorVar: " + resultRunName);
        this.appendLine(builder, indent + 5, "next: returnError");

    }

    private void renderGcpInvokeFunctionYAML(StringBuilder builder, int indent){
        this.appendLine(builder, indent,  FUNCTION_INVOKE_NAME + ":");
        this.appendLine(builder, indent + 1,  "params: [url, jobName, tempLocation, zone]");
        this.appendLine(builder, indent + 1,  "steps:");
        this.appendLine(builder, indent + 2,  "- runInvoke:");
        this.appendLine(builder, indent + 4,  "call: http.post");
        this.appendLine(builder, indent + 4,  "args:");
        this.appendLine(builder, indent + 6,  "url: ${url}");
        this.appendLine(builder, indent + 6,  "auth:");
        this.appendLine(builder, indent + 8,  "type: OAuth2");
        this.appendLine(builder, indent + 6,  "body:");
        this.appendLine(builder, indent + 8,  "jobName: ${jobName}");
        this.appendLine(builder, indent + 8,  "environment:");
        this.appendLine(builder, indent + 10,  "tempLocation: ${tempLocation}");
        this.appendLine(builder, indent + 10,  "zone: ${zone}");
        this.appendLine(builder, indent + 4,  "result: resultData");
        this.appendLine(builder, indent + 2,  "- final:");
        this.appendLine(builder, indent + 4,  "return: ${resultData.body.job.id}");
    }

    private void renderGcpWaitFunctionYAML(StringBuilder builder, int indent) {
        this.appendLine(builder, indent,  FUNCTION_WAIT_NAME + ":");
        this.appendLine(builder, indent + 1,  "params: [execution, project]");
        this.appendLine(builder, indent + 1,  "steps:");
        this.appendLine(builder, indent + 2,  "- init:");
        this.appendLine(builder, indent + 4,  "assign:");
        this.appendLine(builder, indent + 5,  "- i: 0");
        this.appendLine(builder, indent + 5,  "- valid_states: [\"JOB_STATE_RUNNING\"]");
        this.appendLine(builder, indent + 5,  "- result:");
        this.appendLine(builder, indent + 7,  "body:");
        this.appendLine(builder, indent + 9,  "currentState: JOB_STATE_RUNNING");
        this.appendLine(builder, indent + 2,  "- check_condition:");
        this.appendLine(builder, indent + 4,  "switch:");
        this.appendLine(builder, indent + 5,  "- condition: ${result.body.currentState in valid_states AND i<100}");
        this.appendLine(builder, indent + 6,  "next: iterate");
        this.appendLine(builder, indent + 4,  "next: exit_loop");
        this.appendLine(builder, indent + 2,  "- iterate:");
        this.appendLine(builder, indent + 4,  "steps:");
        this.appendLine(builder, indent + 5,  "- sleep:");
        this.appendLine(builder, indent + 7,  "call: sys.sleep");
        this.appendLine(builder, indent + 7,  "args:");
        this.appendLine(builder, indent + 8,  "seconds: " + FUNCTION_WAIT_SECOND);
        this.appendLine(builder, indent + 5,  "- process_item:");
        this.appendLine(builder, indent + 7,  "call: http.get");
        this.appendLine(builder, indent + 7,  "args:");
        this.appendLine(builder, indent + 8,  "url: ${ \"https://dataflow.googleapis.com/v1b3/projects/\" + project + \"/jobs/\" + execution }");
        this.appendLine(builder, indent + 8,  "auth:");
        this.appendLine(builder, indent + 9,  "type: OAuth2");
        this.appendLine(builder, indent + 7,  "result: result");
        this.appendLine(builder, indent + 5,  "- assign_loop:");
        this.appendLine(builder, indent + 7,  "assign:");
        this.appendLine(builder, indent + 8,  "- i: ${i+1}");
        this.appendLine(builder, indent + 8,  "- result: ${result}");
        this.appendLine(builder, indent + 4,  "next: check_condition");
        this.appendLine(builder, indent + 2,  "- exit_loop:");
        this.appendLine(builder, indent + 4,  "return: \"Ok\"");
    }

    private void renderGcpFinisherYAML(StringBuilder builder, int indent, JobEntryNode node) {
        WorkflowFinisherMeta meta = (WorkflowFinisherMeta)node.getEntry();
        this.appendLine(builder, indent, "- return" + this.getName(node) + ":");
        this.appendLine(builder, indent + 2,"return: \"" + this.getValue(meta.getReturnValue()) + "\"");
    }

    private void renderGcpReturnErrorYAML(StringBuilder builder, int indent) {
        this.appendLine(builder, indent, "- returnError:");
        this.appendLine(builder, indent + 2,"return: ${errorVar}");
    }

    //endregion

}
