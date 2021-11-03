/*
 * The MIT License
 *
 * Copyright 2021 Mark A. Hunter.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.ask;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.WorkshopInterface;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelManifest;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelTypeDescriptor;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.DataParcelDirectionEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.DataParcelNormalisationStatusEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.DataParcelValidationStatusEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.PolicyEnforcementPointApprovalStatusEnum;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.edge.ask.beans.HestiaDMJGroupsClient;
import net.fhirfactory.pegacorn.internals.fhir.r4.internal.topics.FHIRElementTopicFactory;
import net.fhirfactory.pegacorn.workshops.EdgeWorkshop;
import net.fhirfactory.pegacorn.wups.archetypes.petasosenabled.messageprocessingbased.MOAStandardWUP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Mark A. Hunter
 */
public class AuditEventAskServiceWUP extends MOAStandardWUP{
    private static final Logger LOG = LoggerFactory.getLogger(AuditEventAskServiceWUP.class);
    
    private boolean subClassInitialised;
    private ObjectMapper jsonMapper;
    
    private static String WUP_VERSION = "1.0.0";

    @Inject
    private FHIRElementTopicFactory fhirElementTopicFactory;
    
    @Inject
    private ProcessingPlantInterface processingPlant;
    
    @Inject
    private EdgeWorkshop workshop;
    
    @Inject
    private HestiaDMJGroupsClient dmClient;
    
    //
    // Constructor(s)
    //
    
    public AuditEventAskServiceWUP(){
        super();
        setSubClassInitialised(false);
    }
    
    //
    // Business Methods
    //

    
    @Override
    public void configure() throws Exception {
        getLogger().info("{}:: ingresFeed() --> {}", getClass().getName(), ingresFeed());
        getLogger().info("{}:: egressFeed() --> {}", getClass().getName(), egressFeed());

        fromIncludingPetasosServices(ingresFeed())
                .routeId(getNameSet().getRouteCoreWUP())
                .bean(dmClient, "persistAuditEvent(*, Exchange)")
                .to(egressFeed());
    }
    
        
    @Override
    protected List<DataParcelManifest> specifySubscriptionTopics() {
        List<DataParcelManifest> subscribedTopics = new ArrayList<>();

        DataParcelTypeDescriptor auditEvent = fhirElementTopicFactory.newTopicToken("AuditEvent", "4.0.1");

        DataParcelManifest manifest = new DataParcelManifest();
        manifest.setContentDescriptor(auditEvent);
        manifest.setNormalisationStatus(DataParcelNormalisationStatusEnum.DATA_PARCEL_CONTENT_NORMALISATION_ANY);
        manifest.setValidationStatus(DataParcelValidationStatusEnum.DATA_PARCEL_CONTENT_VALIDATION_ANY);
        manifest.setSourceSystem("aether-hestia-audit-im");
        manifest.setIntendedTargetSystem("aether-hestia-audit-dm");
        manifest.setEnforcementPointApprovalStatus(PolicyEnforcementPointApprovalStatusEnum.POLICY_ENFORCEMENT_POINT_APPROVAL_ANY);
        manifest.setDataParcelFlowDirection(DataParcelDirectionEnum.INBOUND_DATA_PARCEL);

        subscribedTopics.add(manifest);
        return(subscribedTopics);
    }

    @Override
    protected String specifyWUPInstanceName() {
        return(getClass().getSimpleName());
    }

    @Override
    protected String specifyWUPInstanceVersion() {
        return(WUP_VERSION);
    }

    @Override
    protected WorkshopInterface specifyWorkshop() {
        return(workshop);
    }

        
    //
    // Getters (and Setters)
    //
    
    @Override
    protected Logger specifyLogger() {
        return LOG;
    }

    public boolean isSubClassInitialised() {
        return subClassInitialised;
    }

    public void setSubClassInitialised(boolean subClassInitialised) {
        this.subClassInitialised = subClassInitialised;
    }
    
    protected ProcessingPlantInterface getProcessingPlant(){
        return(processingPlant);
    }
}
