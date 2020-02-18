package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

import java.time.LocalDateTime;

public class StatisticsRootActorControl extends AbstractProtocolParticipantControl<StatisticsRootActorModel>
{
    public StatisticsRootActorControl(StatisticsRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class, this::onRegistrationAcknowledged)
                    .match(StatisticsEvents.DataTransferCompletedEvent.class, this::onDataTransferCompleted)
                    .match(NodeCreationEvents.NodePartitionCreationCompletedEvent.class, this::onNodePartitionCreationCompleted)
                    .match(NodeCreationEvents.NodeCreationCompletedEvent.class, this::onNodeCreationCompleted)
                    .match(EdgeCreationEvents.EdgePartitionCreationCompletedEvent.class, this::onEdgePartitionCreationCompleted)
                    .match(PCAEvents.PrincipalComponentComputationCompletedEvent.class, this::onPrincipalComponentComputationCompleted)
                    .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.DataTransferCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.NodePartitionCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.NodeCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.EdgeCreation, EdgeCreationEvents.EdgePartitionCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.PrincipalComponentAnalysis, PCAEvents.PrincipalComponentComputationCompletedEvent.class);
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);
            getModel().setCalculationStartTime(LocalDateTime.now());
        }
        finally
        {
            complete(message);
        }
    }

    private void onDataTransferCompleted(StatisticsEvents.DataTransferCompletedEvent message)
    {
        try
        {
            getModel().getStatisticsLog().log(this, message);
        }
        finally
        {
            complete(message);
        }
    }

    private void onNodePartitionCreationCompleted(NodeCreationEvents.NodePartitionCreationCompletedEvent message)
    {
        try
        {
            getModel().getStatisticsLog().log(this, message);
        }
        finally
        {
            complete(message);
        }
    }

    private void onNodeCreationCompleted(NodeCreationEvents.NodeCreationCompletedEvent message)
    {
        try
        {
            getModel().getStatisticsLog().log(this, message);
        }
        finally
        {
            complete(message);
        }
    }

    private void onEdgePartitionCreationCompleted(EdgeCreationEvents.EdgePartitionCreationCompletedEvent message)
    {
        try
        {
            getModel().getStatisticsLog().log(this, message);
        }
        finally
        {
            complete(message);
        }
    }

    private void onPrincipalComponentComputationCompleted(PCAEvents.PrincipalComponentComputationCompletedEvent message)
    {
        try
        {
            getModel().getStatisticsLog().log(this, message);
        }
        finally
        {
            complete(message);
        }
    }

    private void onReadyForTermination(ScoringEvents.ReadyForTerminationEvent message)
    {
        try
        {
            getModel().setCalculationEndTime(LocalDateTime.now());
            getModel().getStatisticsLog().log(this, message);
            getModel().getStatisticsLog().close();
        }
        finally
        {
            complete(message);
        }
    }
}
