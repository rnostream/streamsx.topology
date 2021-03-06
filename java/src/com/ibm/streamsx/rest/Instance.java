/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

/**
 * 
 * An object describing an IBM Streams Instance
 * 
 */
public class Instance extends Element {
    
    @Expose
    private String activeServices;
    @Expose
    private ActiveVersion activeVersion;
    @Expose
    private String activeViews;
    @Expose
    private String configuredViews;
    @Expose
    private long creationTime;
    @Expose
    private String creationUser;
    @Expose
    private String domain;
    @Expose
    private String exportedStreams;
    @Expose
    private String health;
    @Expose
    private String hosts;
    @Expose
    private String id;
    @Expose
    private String importedStreams;
    @Expose
    private String jobs;
    @Expose
    private String operatorConnections;
    @Expose
    private String operators;
    @Expose
    private String owner;
    @Expose
    private String peConnections;
    @Expose
    private String pes;
    @Expose
    private String resourceAllocations;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private long startTime;
    @Expose
    private String startedBy;
    @Expose
    private String status;
    @Expose
    private String views;

    static final Instance create(final AbstractStreamsConnection sc, String gsonInstance) {
        Instance instance = gson.fromJson(gsonInstance, Instance.class);
        instance.setConnection(sc);
        return instance;
    }

    final static List<Instance> getInstanceList(AbstractStreamsConnection sc, String instanceGSONList) {
        List<Instance> iList;
        try {
            InstancesArray iArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                    .fromJson(instanceGSONList, InstancesArray.class);

            iList = iArray.instances;
            for (Instance instance : iList) {
                instance.setConnection(sc);
            }

        } catch (JsonSyntaxException e) {
            iList = Collections.<Instance> emptyList();
        }
        return iList;
    }

    /**
     * Gets a list of {@link Job jobs} that this instance knows about
     * 
     * @return List of {@link Job IBM Streams Jobs}
     * @throws IOException
     */
    public List<Job> getJobs() throws IOException {
        String sReturn = connection().getResponseString(jobs);

        List<Job> lJobs = Job.getJobList(this, sReturn);
        return lJobs;
    }

    /**
     * Gets the {@link Job} for a given jobId in this instance
     * 
     * @param jobId
     *            String identifying the job
     * @return a single {@link Job}
     * @throws IOException
     */
    public Job getJob(String jobId) throws IOException {
        requireNonNull(jobId);
        
        String sGetJobURI = jobs + "/" + jobId;

        String sReturn = connection().getResponseString(sGetJobURI);
        Job job = Job.create(this, sReturn);
        return job;
    }

    /**
     * Gets information about the IBM Streams Installation that was used to
     * start this instance
     * 
     * @return {@link ActiveVersion}
     */
    public ActiveVersion getActiveVersion() {
        return activeVersion;
    }

    /**
     * Gets the time in milliseconds when this instance was created
     * 
     * @return the epoch time in milliseconds when the instance was created as a
     *         long
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the user ID that created this instance
     * 
     * @return the creation user ID
     */
    public String getCreationUser() {
        return creationUser;
    }

    /**
     * Gets the summarized status of jobs in this instance
     *
     * @return the summarized status that contains one of the following values:
     *         <ul>
     *         <li>healthy</li>
     *         <li>partiallyHealthy</li>
     *         <li>partiallyUnhealthy</li>
     *         <li>unhealthy</li>
     *         <li>unknown</li>
     *         </ul>
     * 
     */
    public String getHealth() {
        return health;
    }

    /**
     * Gets the IBM Streams unique identifier for this instance
     * 
     * @return the IBM Streams unique idenitifer
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the user ID that represents the owner of this instance
     * 
     * @return the owner user ID
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Identifies the REST resource type
     *
     * @return "instance"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Gets the time in milliseconds when the instance was started.
     * 
     * @return the epoch time in milliseconds when the instance was started as a
     *         long
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Gets the status of the instance
     *
     * @return the instance status that contains one of the following values:
     *         <ul>
     *         <li>running</li>
     *         <li>failed</li>
     *         <li>stopped</li>
     *         <li>partiallyFailed</li>
     *         <li>partiallyRunning</li>
     *         <li>starting</li>
     *         <li>stopping</li>
     *         <li>unknown</li>
     *         </ul>
     * 
     */
    public String getStatus() {
        return status;
    }
    
    private Domain _domain;
    /**
     * Get the Streams domain for this instance.
     * 
     * @return Domain for this instance.
     * 
     * @throws IOException Error communicating with REST api.
     * 
     * @since 1.8
     */
    public Domain getDomain() throws IOException {
        if (_domain == null) {
            String sReturn = connection().getResponseString(domain);
            _domain = Domain.create(connection(), sReturn);
        }
        return _domain;
    }

    /**
     * internal usae to get list of instances
     */
    private static class InstancesArray {
        @Expose
        public ArrayList<Instance> instances;
        @Expose
        public String resourceType;
        @Expose
        public int total;
    }
}
