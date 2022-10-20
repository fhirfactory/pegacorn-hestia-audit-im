package net.fhirfactory.pegacorn.hestia.audit.im.processingplant.configuration;

import net.fhirfactory.pegacorn.core.model.topology.nodes.*;
import net.fhirfactory.pegacorn.core.model.topology.nodes.common.EndpointProviderInterface;
import net.fhirfactory.pegacorn.deployment.properties.configurationfilebased.common.segments.ports.http.HTTPClientPortSegment;
import net.fhirfactory.pegacorn.deployment.topology.factories.archetypes.base.endpoints.HTTPTopologyEndpointFactory;
import net.fhirfactory.pegacorn.deployment.topology.factories.archetypes.fhirpersistence.im.FHIRIMSubsystemTopologyFactory;
import net.fhirfactory.pegacorn.hestia.audit.im.common.HestiaIMNames;
import net.fhirfactory.pegacorn.util.PegacornEnvironmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class HestiaAuditIMTopologyFactory extends FHIRIMSubsystemTopologyFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HestiaAuditIMTopologyFactory.class);

    @Inject
    private HestiaIMNames hestiaIMNames;

    @Inject
    private PegacornEnvironmentProperties pegacornEnvironmentProperties;

    @Inject
    private HTTPTopologyEndpointFactory httpTopologyEndpointFactory;

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    protected Class specifyPropertyFileClass() {
        return (HestiaAuditIMConfigurationFile.class);
    }

    @Override
    protected ProcessingPlantSoftwareComponent buildSubsystemTopology() {
        SubsystemTopologyNode subsystemTopologyNode = buildSubsystemNodeFromConfigurationFile();
        BusinessServiceTopologyNode businessServiceTopologyNode = buildBusinessServiceNode(subsystemTopologyNode);
        DeploymentSiteTopologyNode deploymentSiteTopologyNode = buildDeploymentSiteNode(businessServiceTopologyNode);
        ClusterServiceTopologyNode clusterServiceTopologyNode = buildClusterServiceNode(deploymentSiteTopologyNode,businessServiceTopologyNode);

        PlatformTopologyNode platformTopologyNode = buildPlatformNode(clusterServiceTopologyNode);
        ProcessingPlantSoftwareComponent processingPlantSoftwareComponent = buildProcessingPlant(platformTopologyNode,clusterServiceTopologyNode);
        addPrometheusPort(processingPlantSoftwareComponent);
        addJolokiaPort(processingPlantSoftwareComponent);
        addKubeLivelinessPort(processingPlantSoftwareComponent);
        addKubeReadinessPort(processingPlantSoftwareComponent);
        addEdgeAnswerPort(processingPlantSoftwareComponent);
        addAllJGroupsEndpoints(processingPlantSoftwareComponent);

        // Unique to HestiaIM
        getLogger().trace(".buildSubsystemTopology(): Add the httpFHIRClient port to the ProcessingPlant Topology Node");
        addHTTPClientPorts(processingPlantSoftwareComponent);
        return(processingPlantSoftwareComponent);
    }

    protected void addHTTPClientPorts( EndpointProviderInterface endpointProvider) {
        getLogger().debug(".addHTTPClientPorts(): Entry, endpointProvider->{}", endpointProvider);

        getLogger().trace(".addHTTPClientPorts(): Creating the HTTP Client (Used to Connect-To Hestia Audit DM)");
        HTTPClientPortSegment interactHTTPClient = ((HestiaAuditIMConfigurationFile) getPropertyFile()).getInteractHestiaDMHTTPClient();
        httpTopologyEndpointFactory.newHTTPClientTopologyEndpoint(getPropertyFile(), endpointProvider, hestiaIMNames.getInteractHestiaDMHTTPClientName(),interactHTTPClient );

        getLogger().debug(".addHTTPClientPorts(): Exit");
    }

    protected String specifyPropertyFileName() {
        getLogger().info(".specifyPropertyFileName(): Entry");
        String configurationFileName = pegacornEnvironmentProperties.getMandatoryProperty("DEPLOYMENT_CONFIG_FILE");
        if(configurationFileName == null){
            throw(new RuntimeException("Cannot load configuration file!!!! (SUBSYSTEM-CONFIG_FILE="+configurationFileName+")"));
        }
        getLogger().info(".specifyPropertyFileName(): Exit, filename->{}", configurationFileName);
        return configurationFileName;
    }
}
