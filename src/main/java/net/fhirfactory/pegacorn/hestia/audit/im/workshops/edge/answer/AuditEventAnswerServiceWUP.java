/*
 * Copyright (c) 2021 Mark A. Hunter
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.answer;

import ca.uhn.fhir.rest.api.MethodOutcome;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.fhirfactory.pegacorn.components.capabilities.CapabilityFulfillmentInterface;
import net.fhirfactory.pegacorn.components.capabilities.base.CapabilityUtilisationRequest;
import net.fhirfactory.pegacorn.components.capabilities.base.CapabilityUtilisationResponse;
import net.fhirfactory.pegacorn.components.dataparcel.DataParcelManifest;
import net.fhirfactory.pegacorn.components.interfaces.topology.WorkshopInterface;
import net.fhirfactory.pegacorn.components.transaction.model.SimpleResourceID;
import net.fhirfactory.pegacorn.components.transaction.model.SimpleTransactionOutcome;
import net.fhirfactory.pegacorn.components.transaction.valuesets.TransactionStatusEnum;
import net.fhirfactory.pegacorn.components.transaction.valuesets.TransactionTypeEnum;
import net.fhirfactory.pegacorn.deployment.topology.model.endpoints.base.ExternalSystemIPCEndpoint;
import net.fhirfactory.pegacorn.deployment.topology.model.endpoints.interact.StandardInteractClientTopologyEndpointPort;
import net.fhirfactory.pegacorn.deployment.topology.model.nodes.external.ConnectedExternalSystemTopologyNode;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.answer.beans.HestiaDMHTTPClient;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.answer.beans.MethodOutcome2UoW;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.answer.beans.UoW2AuditEventString;
import net.fhirfactory.pegacorn.internals.fhir.r4.internal.topics.FHIRElementTopicFactory;
import net.fhirfactory.pegacorn.petasos.core.moa.wup.MessageBasedWUPEndpoint;
import net.fhirfactory.pegacorn.wups.archetypes.petasosenabled.messageprocessingbased.InteractEgressMessagingGatewayWUP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.ask.beans.HestiaDMJGroupsClient;
import net.fhirfactory.pegacorn.workshops.EdgeWorkshop;

@ApplicationScoped
public class AuditEventAnswerServiceWUP extends InteractEgressMessagingGatewayWUP implements CapabilityFulfillmentInterface {
    private static final Logger LOG = LoggerFactory.getLogger(AuditEventAnswerServiceWUP.class);

    private static String WUP_VERSION="1.0.0";
    private String CAMEL_COMPONENT_TYPE="netty-http";
    private ObjectMapper jsonMapper;

    public AuditEventAnswerServiceWUP(){
        super();
        jsonMapper = new ObjectMapper();
    }

    @Inject
    private EdgeWorkshop workshop;

    @Inject
    private HestiaDMHTTPClient hestiaDMHTTPClient;
    
    @Inject
    private HestiaDMJGroupsClient hestiaDMJGroupsClient;

    @Inject
    private UoW2AuditEventString uowPayloadExtractor;

    @Inject
    private MethodOutcome2UoW methodOutcome2UoW;

    @Inject
    private FHIRElementTopicFactory fhirTopicFactory;

    @Override
    protected List<DataParcelManifest> specifySubscriptionTopics() {
        return null;
    }

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    protected String specifyWUPInstanceName() {
        return (getClass().getSimpleName());
    }

    @Override
    protected String specifyWUPInstanceVersion() {
        return (WUP_VERSION);
    }

    @Override
    protected WorkshopInterface specifyWorkshop() {
        return (workshop);
    }

    @Override
    public void configure() throws Exception {
        getLogger().info("{}:: ingresFeed() --> {}", getClass().getSimpleName(), ingresFeed());
        getLogger().info("{}:: egressFeed() --> {}", getClass().getSimpleName(), egressFeed());

        fromIncludingPetasosServices(ingresFeed())
                .routeId(getNameSet().getRouteCoreWUP())
                .bean(uowPayloadExtractor, "extractPayload")
                .to(getHestiaAuditDMAccessorPathEntry())
                .bean(methodOutcome2UoW, "encapsulateMethodOutcomeIntoUoW")
                .to(egressFeed());

        from(getHestiaAuditDMAccessorPathEntry())
                .bean(hestiaDMHTTPClient, "writeAuditEvent");
    }

    private String getHestiaAuditDMAccessorPathEntry(){
        String name = "direct:" + getClass().getSimpleName() + "-HestiaAduitDMAccessorPathEntry";
        return(name);
    }

    @Override
    public CapabilityUtilisationResponse executeTask(CapabilityUtilisationRequest request) {
        getLogger().debug(".executeTask(): Entry, request->{}", request);
        String capability = request.getRequiredCapabilityName();
        switch(capability){
            case "FHIR-AuditEvent-Persistence": {
                if(shouldPersistAuditEvent()) {
                    CapabilityUtilisationResponse capabilityUtilisationResponse = executeWriteAuditEventTask(request);
                    return (capabilityUtilisationResponse);
                } else {
                    CapabilityUtilisationResponse capabilityUtilisationResponse = executeFauxWriteAuditEventTask(request);
                    return(capabilityUtilisationResponse);
                }
            }
            default:{
                CapabilityUtilisationResponse response = new CapabilityUtilisationResponse();
                response.setDateCompleted(Instant.now());
                response.setSuccessful(false);
                response.setAssociatedRequestID(request.getRequestID());
                return(response);
            }
        }
    }

    private boolean shouldPersistAuditEvent(){
        String parameterValue = getProcessingPlant().getProcessingPlantNode().getOtherConfigurationParameter("AUDIT_EVENT_PERSISTENCE");
        if(parameterValue != null){
            if(parameterValue.equalsIgnoreCase("true")){
                return(true);
            }
        } else {
            return(false);
        }
        return(false);
    }
    
    private boolean useHadoopDMService(){
        String parameterValue = getProcessingPlant().getProcessingPlantNode().getOtherConfigurationParameter("IM_TO_DM_TECHNOLOGY");
        if(parameterValue != null){
            if(parameterValue.equalsIgnoreCase("jgroups")){
                return(true);
            }
        } else {
            return(false);
        }
        return(false);
    }

    private CapabilityUtilisationResponse executeWriteAuditEventTask(CapabilityUtilisationRequest request){
        String auditEventAsString = request.getRequestContent();
        MethodOutcome methodOutcome = null;
        if(useHadoopDMService()){
            methodOutcome = hestiaDMJGroupsClient.writeAuditEventIntoDM(auditEventAsString);
            
        } else {
            methodOutcome = hestiaDMHTTPClient.writeAuditEvent(auditEventAsString);
        }
        String simpleOutcomeAsString = null;
        SimpleTransactionOutcome simpleOutcome = new SimpleTransactionOutcome();
        SimpleResourceID resourceID = new SimpleResourceID();
        if(methodOutcome.getCreated()) {
            if(methodOutcome.getId() != null) {
                if (methodOutcome.getId().hasResourceType()) {
                    resourceID.setResourceType(methodOutcome.getId().getResourceType());
                } else {
                    resourceID.setResourceType("AuditEvent");
                }
                resourceID.setValue(methodOutcome.getId().getValue());
                if (methodOutcome.getId().hasVersionIdPart()) {
                    resourceID.setVersion(methodOutcome.getId().getVersionIdPart());
                } else {
                    resourceID.setVersion(SimpleResourceID.DEFAULT_VERSION);
                }
                simpleOutcome.setResourceID(resourceID);
            }
            simpleOutcome.setTransactionStatus(TransactionStatusEnum.CREATION_FINISH);
        } else {
            simpleOutcome.setTransactionStatus(TransactionStatusEnum.CREATION_FAILURE);
        }
        simpleOutcome.setTransactionType(TransactionTypeEnum.CREATE);
        simpleOutcome.setTransactionSuccessful(methodOutcome.getCreated());
        try {
            simpleOutcomeAsString = jsonMapper.writeValueAsString(simpleOutcome);
        } catch (JsonProcessingException e) {
            getLogger().warn(".executeWriteAuditEventTask(): Cannot convert MethodOutcome to string, error->",e);
        }
        CapabilityUtilisationResponse response = new CapabilityUtilisationResponse();
        if(simpleOutcomeAsString != null){
            response.setResponseContent(simpleOutcomeAsString);
            response.setSuccessful(true);
        } else {
            response.setSuccessful(false);
        }
        response.setDateCompleted(Instant.now());
        response.setAssociatedRequestID(request.getRequestID());
        return(response);
    }

    private CapabilityUtilisationResponse executeFauxWriteAuditEventTask(CapabilityUtilisationRequest request){
        getLogger().info(request.getRequestContent());
        CapabilityUtilisationResponse response = new CapabilityUtilisationResponse();
        String simpleOutcomeAsAString = null;
        SimpleTransactionOutcome fauxOutcome = new SimpleTransactionOutcome();
        SimpleResourceID resourceID = new SimpleResourceID();
        resourceID.setResourceType("AuditEvent");
        resourceID.setValue(UUID.randomUUID().toString());
        resourceID.setVersion(SimpleResourceID.DEFAULT_VERSION);
        fauxOutcome.setResourceID(resourceID);
        fauxOutcome.setTransactionStatus(TransactionStatusEnum.CREATION_FINISH);
        fauxOutcome.setTransactionType(TransactionTypeEnum.CREATE);
        fauxOutcome.setTransactionSuccessful(true);
        try {
            simpleOutcomeAsAString = jsonMapper.writeValueAsString(fauxOutcome);
        } catch (JsonProcessingException e) {
            getLogger().warn(".executeWriteAuditEventTask(): Cannot convert MethodOutcome to string, error->",e);
        }
        if(simpleOutcomeAsAString != null){
            response.setResponseContent(simpleOutcomeAsAString);
            response.setSuccessful(true);
        } else {
            response.setSuccessful(false);
        }
        response.setDateCompleted(Instant.now());
        response.setAssociatedRequestID(request.getRequestID());
        return(response);
    }

    @Override
    protected String specifyEgressTopologyEndpointName() {
        return ("hestia-dm-http");
    }

    @Override
    protected MessageBasedWUPEndpoint specifyEgressEndpoint() {
        MessageBasedWUPEndpoint endpoint = new MessageBasedWUPEndpoint();
        StandardInteractClientTopologyEndpointPort clientTopologyEndpoint = (StandardInteractClientTopologyEndpointPort) getTopologyEndpoint(specifyEgressTopologyEndpointName());
        ConnectedExternalSystemTopologyNode targetSystem = clientTopologyEndpoint.getTargetSystem();
        ExternalSystemIPCEndpoint externalSystemIPCEndpoint = targetSystem.getTargetPorts().get(0);
        int portValue = externalSystemIPCEndpoint.getTargetPortValue();
        String targetInterfaceDNSName = externalSystemIPCEndpoint.getTargetPortDNSName();
        String httpType = null;
        if(externalSystemIPCEndpoint.getEncryptionRequired()){
            httpType = "https";
        } else {
            httpType = "http";
        }
        endpoint.setEndpointSpecification(CAMEL_COMPONENT_TYPE+":"+httpType+"//"+targetInterfaceDNSName+":"+Integer.toString(portValue)+"?requireEndOfData=false");
        endpoint.setEndpointTopologyNode(clientTopologyEndpoint);
        endpoint.setFrameworkEnabled(false);
        return endpoint;
    }

    @Override
    protected void registerCapabilities(){
        getProcessingPlant().registerCapabilityFulfillmentService("FHIR-AuditEvent-Persistence", this);
    }
}