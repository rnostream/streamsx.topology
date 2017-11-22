/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class RemoteBuildAndSubmitRemoteContext extends ZippedToolkitRemoteContext {
	@Override
    public Type getType() {
        return Type.STREAMING_ANALYTICS_SERVICE;
    }
	
	@Override
	public Future<File> _submit(JsonObject submission) throws Exception {
	    // Get the VCAP service info which also verifies we have the
	    // right information before we do any work.
	    JsonObject deploy = deploy(submission);
        JsonObject vcapServices = VcapServices.getVCAPServices(deploy.get(VCAP_SERVICES));

        final StreamingAnalyticsService sas = StreamingAnalyticsService.of(vcapServices,
                jstring(deploy, SERVICE_NAME));
	    
	    Future<File> archive = super._submit(submission);
	    
	    File buildArchive =  archive.get();
		
	    try {
	        sas.buildAndSubmitJob(buildArchive, submission);
	    } finally {		
		    if (!keepArtifacts(submission))
		        buildArchive.delete();
	    }
		
		return archive;
	}
}
