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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelManifest;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelTypeDescriptor;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.DataParcelDirectionEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.DataParcelNormalisationStatusEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.DataParcelValidationStatusEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.valuesets.PolicyEnforcementPointApprovalStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosFulfillmentTask;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoW;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayload;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWProcessingOutcomeEnum;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MethodOutcome2UoW {
    private static final Logger LOG = LoggerFactory.getLogger(MethodOutcome2UoW.class);

    private ObjectMapper jsonMapper;

    @PostConstruct
    public void initialise(){
        jsonMapper = new ObjectMapper();
    }

    public UoW encapsulateMethodOutcomeIntoUoW(MethodOutcome outcome, Exchange camelExchange){
        LOG.debug(".encapsulateMethodOutcomeIntoUoW(): Entry");

        PetasosFulfillmentTask fulfillmentTask = camelExchange.getProperty(PetasosPropertyConstants.WUP_PETASOS_FULFILLMENT_TASK_EXCHANGE_PROPERTY, PetasosFulfillmentTask.class);
        UoW uowFromExchange = fulfillmentTask.getTaskWorkItem();

        String outcomeAsString = null;
        String failureString = null;
        try {
            outcomeAsString = jsonMapper.writeValueAsString(outcome);
        } catch (JsonProcessingException e) {
            LOG.warn(".encapsulateMethodOutcomeIntoUoW(): Could not convert outcome, error->",e);
            failureString = e.getMessage();
        }

        if(outcomeAsString != null){
            UoWPayload payload = new UoWPayload();
            payload.setPayload(outcomeAsString);
            payload.setPayloadManifest(createManifestForMethodOutcome());
            uowFromExchange.getEgressContent().addPayloadElement(payload);
            uowFromExchange.setProcessingOutcome(UoWProcessingOutcomeEnum.UOW_OUTCOME_SUCCESS);
        } else {
            uowFromExchange.setFailureDescription(failureString);
            uowFromExchange.setProcessingOutcome(UoWProcessingOutcomeEnum.UOW_OUTCOME_FAILED);
        }
        return(uowFromExchange);
    }

    private DataParcelManifest createManifestForMethodOutcome(){
        DataParcelTypeDescriptor descriptor = new DataParcelTypeDescriptor();
        descriptor.setDataParcelDefiner("University Health Network");
        descriptor.setDataParcelCategory("FHIR-REST");
        descriptor.setDataParcelSubCategory("API");
        descriptor.setDataParcelResource("MethodOutcome");
        descriptor.setVersion("R4");

        DataParcelManifest manifest = new DataParcelManifest();
        manifest.setContentDescriptor(descriptor);
        manifest.setDataParcelFlowDirection(DataParcelDirectionEnum.INFORMATION_FLOW_INBOUND_DATA_PARCEL);
        manifest.setValidationStatus(DataParcelValidationStatusEnum.DATA_PARCEL_CONTENT_VALIDATED_TRUE);
        manifest.setNormalisationStatus(DataParcelNormalisationStatusEnum.DATA_PARCEL_CONTENT_NORMALISATION_FALSE);
        manifest.setEnforcementPointApprovalStatus(PolicyEnforcementPointApprovalStatusEnum.POLICY_ENFORCEMENT_POINT_APPROVAL_NEGATIVE);
        manifest.setInterSubsystemDistributable(false);

        return(manifest);
    }
}
