package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import com.sun.management.OperatingSystemMXBean;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
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
                    .match(CreateUtilizationMeasurement.class, this::measureUtilization)
                    .match(StatisticsEvents.StatisticsEvent.class, this::logEvent)
                    .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        getModel().setOsBean(ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class));
        getModel().setMemoryBean(ManagementFactory.getMemoryMXBean());

        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.DataTransferredEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.ProjectionCreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.PCACreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.DimensionReductionCreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.IntersectionsCreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.NodesExtractedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.EdgePartitionCreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.PathScoresCreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.PathScoresNormalizedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.ResultsPersistedEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.MachineUtilizationEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.MessageExchangeUtilizationEvent.class);
        subscribeToLocalEvent(ProtocolType.Statistics, StatisticsEvents.ActorPoolUtilizationEvent.class);
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);
            getModel().setCalculationStartTime(LocalDateTime.now());
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

    private void logEvent(StatisticsEvents.StatisticsEvent message)
    {
        try
        {
            getModel().getStatisticsLog().log(message);
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
            getModel().getStatisticsLog().log(StatisticsEvents.CalculationCompletedEvent.builder()
                                                                                        .sender(getModel().getSelf())
                                                                                        .receiver(getModel().getSelf())
                                                                                        .startTime(getModel().getCalculationStartTime())
                                                                                        .endTime(getModel().getCalculationEndTime())
                                                                                        .build());

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
