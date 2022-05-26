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
package net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.ask;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.fhirfactory.pegacorn.core.interfaces.auditing.PetasosAuditEventServiceClientWriterInterface;
import net.fhirfactory.pegacorn.core.interfaces.capabilities.CapabilityFulfillmentInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.WorkshopInterface;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelManifest;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.adapters.HTTPClientAdapter;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.http.HTTPClientTopologyEndpoint;
import net.fhirfactory.pegacorn.hestia.audit.im.common.HestiaIMNames;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.ask.beans.HestiaDMHTTPClient;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.ask.beans.MethodOutcome2UoW;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.ask.beans.UoW2AuditEventString;
import net.fhirfactory.pegacorn.petasos.core.moa.wup.MessageBasedWUPEndpointContainer;
import net.fhirfactory.pegacorn.workshops.EdgeWorkshop;
import net.fhirfactory.pegacorn.wups.archetypes.petasosenabled.messageprocessingbased.InteractEgressMessagingGatewayWUP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class AuditEventAskServiceWUP extends InteractEgressMessagingGatewayWUP {
    private static final Logger LOG = LoggerFactory.getLogger(AuditEventAskServiceWUP.class);

    private static String WUP_VERSION="1.0.0";
    private String CAMEL_COMPONENT_TYPE="netty-http";
    private ObjectMapper jsonMapper;

    public AuditEventAskServiceWUP(){
        super();
        jsonMapper = new ObjectMapper();
    }

    @Inject
    private EdgeWorkshop workshop;

    @Inject
    private HestiaDMHTTPClient hestiaDMHTTPClient;

    @Inject
    private UoW2AuditEventString uowPayloadExtractor;

    @Inject
    private MethodOutcome2UoW methodOutcome2UoW;

    @Inject
    private HestiaIMNames hestiaIMNames;

    @Inject
    private PetasosAuditEventServiceClientWriterInterface auditEventWriter;

    @Inject
    private CapabilityFulfillmentInterface capabilityFulfillmentInterface;

    @Override
    protected List<DataParcelManifest> specifySubscriptionTopics() {
        return (new ArrayList<>());
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

    //
    // Framework Methods
    //

    @Override
    protected String specifyEgressTopologyEndpointName() {
        return (hestiaIMNames.getInteractHestiaDMHTTPClientName());
    }

    @Override
    protected MessageBasedWUPEndpointContainer specifyEgressEndpoint() {
        MessageBasedWUPEndpointContainer endpoint = new MessageBasedWUPEndpointContainer();
        HTTPClientTopologyEndpoint clientTopologyEndpoint = (HTTPClientTopologyEndpoint) getTopologyEndpoint(specifyEgressTopologyEndpointName());
        HTTPClientAdapter externalSystemIPCAdapter = (HTTPClientAdapter)clientTopologyEndpoint.getTargetSystem().getTargetPorts().get(0);
        int portValue = externalSystemIPCAdapter.getPortNumber();
        String targetInterfaceDNSName = externalSystemIPCAdapter.getHostName();
        String httpType = null;
        if(externalSystemIPCAdapter.isEncrypted()){
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
        getProcessingPlant().registerCapabilityFulfillmentService("FHIR-AuditEvent-Persistence", capabilityFulfillmentInterface);
    }

    @Override
    protected List<DataParcelManifest> declarePublishedTopics() {
        return (new ArrayList<>());
    }

    @Override
    protected String specifyEndpointParticipantName() {
        return ("Hestia.Audit.IM");
    }
}
