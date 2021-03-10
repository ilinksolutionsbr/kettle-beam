package org.kettle.beam.job.pipeline;

import org.pentaho.di.job.entry.JobEntryInterface;

public class JobEntryNode {

    private String name;
    private JobEntryInterface entry;
    private JobEntryNode previous;
    private JobEntryNode next;


    public JobEntryNode(String name, JobEntryInterface entry){
        this.name = name;
        this.entry = entry;
    }


    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public JobEntryInterface getEntry() {
        return entry;
    }
    public void setEntry(JobEntryInterface entry) {
        this.entry = entry;
    }

    public JobEntryNode getPrevious() {
        return previous;
    }
    public void setPrevious(JobEntryNode previous) {
        this.previous = previous;
    }

    public JobEntryNode getNext() {
        return next;
    }
    public void setNext(JobEntryNode next) {
        this.next = next;
    }

}
