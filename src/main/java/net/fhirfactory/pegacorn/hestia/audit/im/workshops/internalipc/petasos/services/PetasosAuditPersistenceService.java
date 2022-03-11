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
package net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.petasos.services;

import ca.uhn.fhir.rest.api.MethodOutcome;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.fhirfactory.pegacorn.core.interfaces.auditing.PetasosAuditEventServiceAgentInterface;
import net.fhirfactory.pegacorn.core.interfaces.auditing.PetasosAuditEventServiceBrokerInterface;
import net.fhirfactory.pegacorn.core.interfaces.auditing.PetasosAuditEventServiceClientWriterInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.datagrid.AsynchronousWriterAuditEventCache;
import net.fhirfactory.pegacorn.hestia.audit.im.workshops.internalipc.ask.beans.HestiaDMHTTPClient;
import org.hl7.fhir.r4.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

@ApplicationScoped
public class PetasosAuditPersistenceService implements PetasosAuditEventServiceClientWriterInterface,
        PetasosAuditEventServiceBrokerInterface, PetasosAuditEventServiceAgentInterface {
    private static final Logger LOG = LoggerFactory.getLogger(PetasosAuditPersistenceService.class);

    private ObjectMapper jsonMapper;

    private boolean stillRunning;
    private Object writerLock;

    private Long ASYNC_AUDIT_WRITER_STARTUP_DELAY = 60000L;
    private Long ASYNC_AUDIT_WRITER_CHECK_PERIOD = 10000L;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private HestiaDMHTTPClient hestiaDMHTTPClient;

    @Inject
    private AsynchronousWriterAuditEventCache eventCache;

    //
    // Constructor(s)
    //

    public PetasosAuditPersistenceService() {
        jsonMapper = new ObjectMapper();
        stillRunning = false;
        writerLock = new Object();
        scheduleAsynchronousAuditEventWriterDaemon();
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger() {
        return (LOG);
    }

    protected ProcessingPlantInterface getProcessingPlant() {
        return (this.processingPlant);
    }

    protected ObjectMapper getJSONMapper() {
        return (jsonMapper);
    }

    protected HestiaDMHTTPClient getHestiaDMHTTPClient() {
        return (hestiaDMHTTPClient);
    }

    protected AsynchronousWriterAuditEventCache getAuditEventCache(){
        return(eventCache);
    }

    protected Object getWriterLock(){
        return(writerLock);
    }

    //
    // Global Audit Event Services
    //


    @Override
    public MethodOutcome writeAuditEventJSONStringSynchronously(String auditEventJSONString) {
        getLogger().debug(".writeAuditEventJSONStringSynchronously(): Entry, auditEvent->{}", auditEventJSONString);
        MethodOutcome methodOutcome;

        synchronized (getWriterLock()) {
            methodOutcome = getHestiaDMHTTPClient().writeAuditEvent(auditEventJSONString);
        }
        getLogger().debug(".writeAuditEventJSONStringSynchronously(): Exit, methodOutcome->{}", methodOutcome);
        return(methodOutcome);
    }

    @Override
    public MethodOutcome writeAuditEventAsynchronously(AuditEvent auditEvent) {
        getLogger().debug(".writeAuditEventAsynchronously(): Entry, auditEvent->{}", auditEvent);
        MethodOutcome outcome = writeAuditEvent(auditEvent);
        getLogger().debug(".writeAuditEventAsynchronously(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    @Override
    public MethodOutcome writeAuditEventSynchronously(AuditEvent auditEvent) {
        getLogger().debug(".writeAuditEventSynchronously(): Entry, auditEvent->{}", auditEvent);
        MethodOutcome outcome = writeAuditEvent(auditEvent);
        getLogger().debug(".writeAuditEventSynchronously(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    //
    // Actual Writing Invocation Function
    //

    public MethodOutcome writeAuditEvent(AuditEvent auditEvent) {
        getLogger().debug(".writeAuditEvent(): Entry, auditEvent->{}", auditEvent);
        MethodOutcome outcome = null;
        if(auditEvent != null) {
            getLogger().debug(".writeAuditEvent(): AuditEvent is not -null-, writing!");
            synchronized (getWriterLock()) {
                getLogger().debug(".writeAuditEvent(): Got Writing Semaphore, writing!");
                outcome = getHestiaDMHTTPClient().writeAuditEvent(auditEvent);
            }
        }
        getLogger().debug(".writeAuditEvent(): Exit, auditEvent->{}", auditEvent);
        return (outcome);
    }

    //
    // Local Audit Event Broker Services
    //

    @Override
    public Boolean logAuditEvent(String serviceProviderName, AuditEvent event) {
        getLogger().debug(".logAuditEvent(): Entry, auditEvent->{}", event);
        MethodOutcome outcome = writeAuditEvent(event);
        Boolean success = false;
        if(outcome != null){
            success = outcome.getCreated();
        }
        getLogger().debug(".logAuditEvent(): Exit, success->{}", success);
        return(success);
    }

    @Override
    public Boolean logAuditEvent(String serviceProviderName, List<AuditEvent> eventList) {
        getLogger().debug(".logAuditEvent(): Entry, auditEvent->{}", eventList);
        Boolean success = false;
        if(eventList != null){
            if(!eventList.isEmpty()){
                for(AuditEvent currentEvent: eventList){
                    getAuditEventCache().addAuditEvent(currentEvent);
                }
            }
        }
        success = true;
        getLogger().debug(".logAuditEvent(): Exit, success->{}", success);
        return(success);
    }

    //
    // Helper Functions
    //

    private boolean useHadoopDMService() {
        String parameterValue = getProcessingPlant().getMeAsASoftwareComponent().getOtherConfigurationParameter("IM_TO_DM_TECHNOLOGY");
        if (parameterValue != null) {
            if (parameterValue.equalsIgnoreCase("jgroups")) {
                return (true);
            }
        } else {
            return (false);
        }
        return (false);
    }

    //
    // Asynchronous Writer Daemon
    //

    //
    // Scheduler

    private void scheduleAsynchronousAuditEventWriterDaemon() {
        getLogger().debug(".scheduleAsynchronousAuditEventWriterDaemon(): Entry");
        TimerTask asynchronousAuditEventWriterDaemon = new TimerTask() {
            public void run() {
                getLogger().debug(".asynchronousAuditEventWriterDaemon(): Entry");
                asynchronousAuditEventWriterTask();
                getLogger().debug(".asynchronousAuditEventWriterDaemon(): Exit");
            }
        };
        Timer timer = new Timer("AsynchronousAuditEventTimer");
        timer.schedule(asynchronousAuditEventWriterDaemon, ASYNC_AUDIT_WRITER_STARTUP_DELAY, ASYNC_AUDIT_WRITER_CHECK_PERIOD);
        getLogger().debug(".scheduleAsynchronousAuditEventWriterDaemon(): Exit");
    }

    //
    // Task

    private void asynchronousAuditEventWriterTask(){
        getLogger().debug(".notificationForwarder(): Entry");
        stillRunning = true;

        while(getAuditEventCache().hasEntries()) {
            getLogger().trace(".notificationForwarder(): Entry");
            AuditEvent currentAuditEvent = getAuditEventCache().peekAuditEvent();
            MethodOutcome outcome = null;
            synchronized (getWriterLock()) {
                outcome = hestiaDMHTTPClient.writeAuditEvent(currentAuditEvent);
            }
            boolean success = false;
            if(outcome != null) {
                if (outcome.getCreated()) {
                    getAuditEventCache().pollAuditEvent();
                    success = true;
                }
            }
            if(!success){
                getLogger().warn(".asynchronousAuditEventWriterTask(): Failed to write AuditEvent!");
                break;
            }
        }
        stillRunning = false;
        getLogger().debug(".notificationForwarder(): Exit");
    }

    //
    // Audit Client Service
    //


    @Override
    public Boolean captureAuditEvent(AuditEvent event, boolean synchronous) {
        Boolean success = logAuditEvent(getProcessingPlant().getSubsystemParticipantName(), event);
        return(success);
    }
}
