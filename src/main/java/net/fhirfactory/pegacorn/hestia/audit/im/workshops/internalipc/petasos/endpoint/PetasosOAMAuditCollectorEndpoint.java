/*
 * Copyright (c) 2021 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.petasos.endpoint;

import ca.uhn.fhir.rest.api.MethodOutcome;
import com.fasterxml.jackson.core.JsonProcessingException;
import net.fhirfactory.pegacorn.core.interfaces.auditing.PetasosAuditEventServiceClientWriterInterface;
import net.fhirfactory.pegacorn.core.interfaces.auditing.PetasosAuditEventServiceHandlerInterface;
import net.fhirfactory.pegacorn.core.interfaces.capabilities.CapabilityFulfillmentInterface;
import net.fhirfactory.pegacorn.core.interfaces.capabilities.CapabilityProviderNameServiceInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.capabilities.base.CapabilityUtilisationRequest;
import net.fhirfactory.pegacorn.core.model.capabilities.base.CapabilityUtilisationResponse;
import net.fhirfactory.pegacorn.core.model.capabilities.base.factories.MethodOutcomeFactory;
import net.fhirfactory.pegacorn.core.model.capabilities.valuesets.WorkUnitProcessorCapabilityEnum;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.edge.jgroups.JGroupsIntegrationPointSummary;
import net.fhirfactory.pegacorn.core.model.topology.role.ProcessingPlantRoleEnum;
import net.fhirfactory.pegacorn.core.model.transaction.factories.PegacornTransactionMethodOutcomeFactory;
import net.fhirfactory.pegacorn.core.model.transaction.model.PegacornTransactionMethodOutcome;
import net.fhirfactory.pegacorn.core.model.transaction.model.PegacornTransactionOutcome;
import net.fhirfactory.pegacorn.core.model.transaction.model.SimpleResourceID;
import net.fhirfactory.pegacorn.core.model.transaction.valuesets.PegacornTransactionStatusEnum;
import net.fhirfactory.pegacorn.core.model.transaction.valuesets.PegacornTransactionTypeEnum;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.datagrid.AsynchronousWriterAuditEventCache;
import net.fhirfactory.pegacorn.petasos.endpoints.services.audit.PetasosAuditServicesEndpoint;
import net.fhirfactory.pegacorn.util.FHIRContextUtility;
import org.hl7.fhir.r4.model.AuditEvent;
import org.hl7.fhir.r4.model.IdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class PetasosOAMAuditCollectorEndpoint extends PetasosAuditServicesEndpoint
        implements  PetasosAuditEventServiceHandlerInterface, CapabilityFulfillmentInterface {
    private static final Logger LOG = LoggerFactory.getLogger(PetasosOAMAuditCollectorEndpoint.class);

    @Inject
    private PetasosAuditEventServiceClientWriterInterface auditEventWriter;

    @Inject
    private PegacornTransactionMethodOutcomeFactory outcomeFactory;

    @Inject
    private AsynchronousWriterAuditEventCache auditEventCache;

    @Inject
    private MethodOutcomeFactory methodOutcomeFactory;

    @Inject
    private CapabilityProviderNameServiceInterface capabilityProviderNameServiceInterface;

    //
    // Constructor(s)
    //

    public PetasosOAMAuditCollectorEndpoint(){
        super();
    }

    //
    // Post Construct (invoked from Superclass)
    //

    @Override
    protected void executePostConstructInstanceActivities(){
        registerCapability();
    }

    //
    // Getters (and Setters)
    //

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    //
    // AuditEvent RPC Method Support
    //

    @Override
    public Boolean logAuditEventHandler(AuditEvent event, JGroupsIntegrationPointSummary sourceJGroupsIP){
        getLogger().debug(".logAuditEventHandler(): Entry, event->{}, sourceJGroupsIP->{}", event, sourceJGroupsIP);
        MethodOutcome outcome = null;
        if((event != null)) {
            getLogger().debug(".logAuditEventHandler(): Event is not -null-, writing it to the DM");
            outcome = auditEventWriter.writeAuditEventSynchronously(event);
        }
        Boolean success = false;
        if(outcome != null){
            if(outcome.getCreated()){
                success = true;
            }
        }
        getMetricsAgent().incrementRemoteProcedureCallHandledCount();
        getLogger().debug(".logAuditEventHandler(): Exit, success->{}", success);
        return(success);
    }

    @Override
    public Boolean logAuditEventAsynchronouslyHandler(AuditEvent event, JGroupsIntegrationPointSummary jgroupsIP) {
        getLogger().debug(".logAuditEventAsynchronouslyHandler(): Entry, event->{}, sourceJGroupsIP->{}", event, jgroupsIP);
        Boolean success = false;
        if(event != null) {
            getLogger().debug(".logAuditEventAsynchronouslyHandler(): Event is not -null-, adding it to queue");
            auditEventCache.addAuditEvent(event);
            success = true;
        }
        getMetricsAgent().incrementRemoteProcedureCallHandledCount();
        getLogger().debug(".logAuditEventAsynchronouslyHandler(): Exit, success->{}", success);
        return(success);
    }

    @Override
    public Boolean logMultipleAuditEventHandler(List<AuditEvent> eventList, JGroupsIntegrationPointSummary jgroupsIP){
        getLogger().debug(".logMultipleAuditEventHandler(): Entry, eventList->{}, jgroupsIP->{}", eventList, jgroupsIP);
        Boolean success = false;
        if(eventList != null) {
            getLogger().debug(".logMultipleAuditEventHandler(): EventList is not -null-, adding entries to queue");
            for (AuditEvent currentAuditEvent : eventList) {
                auditEventCache.addAuditEvent(currentAuditEvent);
            }
        }
        success = true;
        getMetricsAgent().incrementRemoteProcedureCallHandledCount();
        getLogger().debug(".logMultipleAuditEventHandler(): Exit, success->{}", success);
        return(success);
    }

    //
    // Capability Execution Service
    //

    public void registerCapability(){
        getProcessingPlant().registerCapabilityFulfillmentService(WorkUnitProcessorCapabilityEnum.CAPABILITY_INFORMATION_MANAGEMENT_AUDIT_EVENTS.getDisplayName(), this);
    }

    public CapabilityUtilisationResponse executeTask(CapabilityUtilisationRequest request) {
        String auditEventAsString = request.getRequestStringContent();
        MethodOutcome methodOutcome = null;
        try {
            AuditEvent auditEvent = getFHIRJSONParser().parseResource(AuditEvent.class, auditEventAsString);
            methodOutcome = auditEventWriter.writeAuditEventSynchronously(auditEvent);
        } catch (Exception ex){
            methodOutcome = new MethodOutcome();
            methodOutcome.setCreated(false);
        }
        String simpleOutcomeAsString = null;
        PegacornTransactionOutcome simpleOutcome = new PegacornTransactionOutcome();
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
            simpleOutcome.setTransactionStatus(PegacornTransactionStatusEnum.CREATION_FINISH);
        } else {
            simpleOutcome.setTransactionStatus(PegacornTransactionStatusEnum.CREATION_FAILURE);
        }
        simpleOutcome.setTransactionType(PegacornTransactionTypeEnum.CREATE);
        simpleOutcome.setTransactionSuccessful(methodOutcome.getCreated());
        CapabilityUtilisationResponse response = new CapabilityUtilisationResponse();
        response.setInstantCompleted(Instant.now());
        response.setAssociatedRequestID(request.getRequestID());
        return(response);
    }
}
