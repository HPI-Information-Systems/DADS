package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import com.sun.management.OperatingSystemMXBean;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolEvents;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalDateTime;

public class StatisticsRootActorControl extends AbstractProtocolParticipantControl<StatisticsRootActorModel>
{
    private static class CreateUtilizationMeasurement implements Serializable
    {
        private static final long serialVersionUID = 3678205715843321069L;
    }

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
                    .match(SequenceSliceDistributionEvents.ProjectionCreationCompletedEvent.class, this::onProjectionCreationCompleted)
                    .match(NodeCreationEvents.NodePartitionCreationCompletedEvent.class, this::onNodePartitionCreationCompleted)
                    .match(NodeCreationEvents.NodeCreationCompletedEvent.class, this::onNodeCreationCompleted)
                    .match(EdgeCreationEvents.EdgePartitionCreationCompletedEvent.class, this::onEdgePartitionCreationCompleted)
                    .match(PCAEvents.PrincipalComponentComputationCompletedEvent.class, this::onPrincipalComponentComputationCompleted)
                    .match(DimensionReductionEvents.DimensionReductionCompletedEvent.class, this::onDimensionReductionCompleted)
                    .match(ScoringEvents.PathScoringCompletedEvent.class, this::onPathScoringCompleted)
                    .match(ScoringEvents.PathScoreNormalizationCompletedEvent.class, this::onPathScoreNormalizationCompleted)
                    .match(CreateUtilizationMeasurement.class, this::measureUtilization)
                    .match(StatisticsEvents.MachineUtilizationEvent.class, this::onUtilization)
                    .match(MessageExchangeEvents.UtilizationEvent.class, this::onMessageExchangeUtilization)
                    .match(ActorPoolEvents.UtilizationEvent.class, this::onActorPoolUtilization)
                    .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        getModel().setOsBean(ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class));
        getModel().setMemoryBean(ManagementFactory.getMemoryMXBean());

        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.DataTransferCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.SequenceSliceDistribution, SequenceSliceDistributionEvents.ProjectionCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.DimensionReduction, DimensionReductionEvents.DimensionReductionCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.NodePartitionCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.NodeCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.EdgeCreation, EdgeCreationEvents.EdgePartitionCreationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.PrincipalComponentAnalysis, PCAEvents.PrincipalComponentComputationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.Scoring, ScoringEvents.PathScoringCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.Scoring, ScoringEvents.PathScoreNormalizationCompletedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.MachineUtilizationEvent.class);
        subscribeToLocalEvent(ProtocolType.MessageExchange, MessageExchangeEvents.UtilizationEvent.class);
        subscribeToLocalEvent(ProtocolType.ActorPool, ActorPoolEvents.UtilizationEvent.class);
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);
            getModel().setCalculationStartTime(LocalDateTime.now());

            getModel().getStatisticsLog().log(this, message);
            startMeasuringUtilization();
        }
        finally
        {
            complete(message);
        }
    }

    private void startMeasuringUtilization()
    {
        if (getModel().getMeasureUtilizationTask() != null)
        {
            getModel().getMeasureUtilizationTask().cancel();
        }

        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        assert scheduler != null : "Scheduler must not be null!";
        assert dispatcher != null : "Dispatcher must not be null!";

        val task = scheduler.scheduleAtFixedRate(Duration.ZERO,
                                                 StatisticsProtocol.MEASUREMENT_INTERVAL,
                                                 () -> getModel().getSelf().tell(new CreateUtilizationMeasurement(), getModel().getSelf()),
                                                 dispatcher);
        getModel().setMeasureUtilizationTask(task);
    }

    private void measureUtilization(CreateUtilizationMeasurement message)
    {
        val heapUsage = getModel().getMemoryBean().getHeapMemoryUsage();
        val stackUsage = getModel().getMemoryBean().getNonHeapMemoryUsage();

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.MachineUtilizationEvent.builder()
                                                                                                         .sender(getModel().getSelf())
                                                                                                         .receiver(eventDispatcher)
                                                                                                         .dateTime(LocalDateTime.now())
                                                                                                         .maximumMemoryInBytes(heapUsage.getMax())
                                                                                                         .usedHeapInBytes(heapUsage.getUsed())
                                                                                                         .usedStackInBytes(stackUsage.getUsed())
                                                                                                         .cpuUtilization(getModel().getOsBean().getProcessCpuLoad())
                                                                                                         .build());
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

    private void onProjectionCreationCompleted(SequenceSliceDistributionEvents.ProjectionCreationCompletedEvent message)
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

    private void onDimensionReductionCompleted(DimensionReductionEvents.DimensionReductionCompletedEvent message)
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

    private void onPathScoringCompleted(ScoringEvents.PathScoringCompletedEvent message)
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

    private void onPathScoreNormalizationCompleted(ScoringEvents.PathScoreNormalizationCompletedEvent message)
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

    private void onUtilization(StatisticsEvents.MachineUtilizationEvent message)
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

    private void onMessageExchangeUtilization(MessageExchangeEvents.UtilizationEvent message)
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

    private void onActorPoolUtilization(ActorPoolEvents.UtilizationEvent message)
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

            if (getModel().getMeasureUtilizationTask() != null)
            {
                getModel().getMeasureUtilizationTask().cancel();
            }

            getModel().getStatisticsLog().close();

        }
        finally
        {
            complete(message);
        }
    }
}
