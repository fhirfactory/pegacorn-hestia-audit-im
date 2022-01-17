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
package net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.answer.beans;

import ca.uhn.fhir.rest.api.MethodOutcome;
import net.fhirfactory.pegacorn.core.constants.systemwide.PegacornReferenceProperties;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.componentid.TopologyNodeFDN;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.base.IPCTopologyEndpoint;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.interact.ExternalSystemIPCAdapter;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.interact.StandardInteractClientTopologyEndpointPort;
import net.fhirfactory.pegacorn.core.model.topology.nodes.external.ConnectedExternalSystemTopologyNode;
import net.fhirfactory.pegacorn.deployment.topology.manager.TopologyIM;
import net.fhirfactory.pegacorn.hestia.audit.im.common.HestiaIMNames;
import net.fhirfactory.pegacorn.hestia.audit.im.processingplant.configuration.HestiaAuditIMTopologyFactory;
import net.fhirfactory.pegacorn.petasos.core.moa.wup.MessageBasedWUPEndpointContainer;
import net.fhirfactory.pegacorn.platform.edge.ask.base.http.InternalFHIRClientProxy;
import org.hl7.fhir.r4.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;

@ApplicationScoped
public class HestiaDMHTTPClient extends InternalFHIRClientProxy {
    private static final Logger LOG = LoggerFactory.getLogger(HestiaDMHTTPClient.class);

    private boolean resolvedAuditPersistenceValue;
    private boolean auditPersistence;

    @Inject
    HestiaAuditIMTopologyFactory topologyFactory;

    @Inject
    private HestiaIMNames hestiaIMNames;

    @Override
    protected Logger getLogger() {
        return (LOG);
    }

    @Inject
    private TopologyIM topologyIM;

    @Inject
    private PegacornReferenceProperties systemWideProperties;

    @Inject
    private ProcessingPlantInterface processingPlant;

    public HestiaDMHTTPClient(){
        super();
        resolvedAuditPersistenceValue = false;
        auditPersistence = false;
    }

    @Override
    protected String deriveTargetEndpointDetails(){
        getLogger().debug(".deriveTargetEndpointDetails(): Entry");
        MessageBasedWUPEndpointContainer endpoint = new MessageBasedWUPEndpointContainer();
        StandardInteractClientTopologyEndpointPort clientTopologyEndpoint = (StandardInteractClientTopologyEndpointPort) getTopologyEndpoint(hestiaIMNames.getInteractHestiaDMHTTPClientName());
        ConnectedExternalSystemTopologyNode targetSystem = clientTopologyEndpoint.getTargetSystem();
        ExternalSystemIPCAdapter externalSystemIPCAdapter = (ExternalSystemIPCAdapter) targetSystem.getTargetPorts().get(0);
        String http_type = null;
        if(externalSystemIPCAdapter.getEncryptionRequired()) {
            http_type = "https";
        } else {
            http_type = "http";
        }
        String dnsName = externalSystemIPCAdapter.getTargetPortDNSName();
        String portNumber = Integer.toString(externalSystemIPCAdapter.getTargetPortValue());
        String endpointDetails = http_type + "://" + dnsName + ":" + portNumber + systemWideProperties.getPegacornInternalFhirResourceR4Path();
        getLogger().debug(".deriveTargetEndpointDetails(): Exit, endpointDetails --> {}", endpointDetails);
        return(endpointDetails);
    }

    protected IPCTopologyEndpoint getTopologyEndpoint(String topologyEndpointName){
        getLogger().debug(".getTopologyEndpoint(): Entry, topologyEndpointName->{}", topologyEndpointName);
        ArrayList<TopologyNodeFDN> endpointFDNs = processingPlant.getMeAsASoftwareComponent().getEndpoints();
        for(TopologyNodeFDN currentEndpointFDN: endpointFDNs){
            IPCTopologyEndpoint endpointTopologyNode = (IPCTopologyEndpoint)topologyIM.getNode(currentEndpointFDN);
            if(endpointTopologyNode.getEndpointConfigurationName().contentEquals(topologyEndpointName)){
                getLogger().debug(".getTopologyEndpoint(): Exit, node found -->{}", endpointTopologyNode);
                return(endpointTopologyNode);
            }
        }
        getLogger().debug(".getTopologyEndpoint(): Exit, Could not find node!");
        return(null);
    }

    public MethodOutcome writeAuditEvent(String auditEventJSONString){
        getLogger().debug(".writeAuditEvent(): Entry, auditEventJSONString->{}", auditEventJSONString);
        MethodOutcome outcome = null;
        if(persistAuditEvent()){
            getLogger().info(".writeAuditEvent(): Writing to Hestia-Audit-DM");
            // write the event to the Persistence service
            AuditEvent auditEvent = getFHIRContextUtility().getJsonParser().parseResource(AuditEvent.class, auditEventJSONString);
            outcome = writeAuditEvent(auditEvent);
        } else {
            getLogger().info(auditEventJSONString);
        }
        getLogger().info(".writeAuditEvent(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome writeAuditEvent(AuditEvent auditEvent){
        getLogger().debug(".writeAuditEvent(): Entry, auditEvent->{}", auditEvent);
        MethodOutcome outcome = null;
        try {
            outcome = getClient().create()
                    .resource(auditEvent)
                    .prettyPrint()
                    .encodedJson()
                    .execute();
        } catch (Exception ex){
            getLogger().error(".writeAuditEvent(): ", ex);
            outcome = new MethodOutcome();
            outcome.setCreated(false);
        }
        getLogger().info(".writeAuditEvent(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    private boolean persistAuditEvent(){
        if(!this.resolvedAuditPersistenceValue){
            String auditEventPersistenceValue = processingPlant.getMeAsASoftwareComponent().getOtherConfigurationParameter("AUDIT_EVENT_PERSISTENCE");
            if (auditEventPersistenceValue.equalsIgnoreCase("true")) {
                this.auditPersistence = true;
            } else {
                this.auditPersistence = false;
            }
            this.resolvedAuditPersistenceValue = true;
        }
        return(this.auditPersistence);
    }

    @Override
    protected void postConstructActivities() {

    }
}
