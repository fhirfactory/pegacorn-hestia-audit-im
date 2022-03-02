package net.fhirfactory.pegacorn.hestia.audit.im.workshops.datagrid;

import org.hl7.fhir.r4.model.AuditEvent;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.ConcurrentLinkedQueue;

@ApplicationScoped
public class AsynchronousWriterAuditEventCache {

    private ConcurrentLinkedQueue<AuditEvent> eventQueue;

    //
    // Constructor
    //

    public AsynchronousWriterAuditEventCache(){
        this.eventQueue = new ConcurrentLinkedQueue<>();
    }

    //
    // Getters (and Setters)
    //


    public ConcurrentLinkedQueue<AuditEvent> getEventQueue() {
        return eventQueue;
    }

    //
    // Basic Methods
    //

    public void addAuditEvent(AuditEvent auditEvent){
        getEventQueue().offer(auditEvent);
    }

    public AuditEvent peekAuditEvent(){
        AuditEvent nextEvent = getEventQueue().peek();
        return(nextEvent);
    }

    public AuditEvent pollAuditEvent(){
        AuditEvent nextEvent = getEventQueue().poll();
        return(nextEvent);
    }

    public boolean hasEntries(){
        boolean hasAtLeastOneEntry = !(getEventQueue().isEmpty());
        return(hasAtLeastOneEntry);
    }
}
