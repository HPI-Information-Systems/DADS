package de.hpi.msc.jschneider.protocol.statistics;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.statistics.rootActor.StatisticsLog;
import de.hpi.msc.jschneider.protocol.statistics.rootActor.StatisticsRootActorControl;
import de.hpi.msc.jschneider.protocol.statistics.rootActor.StatisticsRootActorModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Set;

public class StatisticsProtocol
{
    public static final Duration MEASUREMENT_INTERVAL = Duration.ofMillis(50);
    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");

    private static final Logger Log = LogManager.getLogger(StatisticsProtocol.class);
    private static final String ROOT_ACTOR_NAME = "StatisticsRootActor";
    private static final String EVENT_DISPATCHER_NAME = "StatisticsEventDispatcher";

    public static void initializeInPlace(Set<Protocol> localProtocols, ActorSystem actorSystem)
    {
        if (SystemParameters.getCommand().isDisableStatistics())
        {
            return;
        }

        if (SystemParameters.getCommand().getStatisticsFile() == null)
        {
            throw new IllegalArgumentException("Missing parameter: StatisticsOutput!");
        }

        localProtocols.add(initialize(actorSystem));
    }

    private static Protocol initialize(ActorSystem actorSystem)
    {
        val localProtocol = BaseProtocol.builder()
                                        .type(ProtocolType.Statistics)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info("{} successfully initialized.", StatisticsProtocol.class.getName());
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = StatisticsRootActorModel.builder()
                                            .statisticsLog(new StatisticsLog(SystemParameters.getCommand().getStatisticsFile().toFile()))
                                            .schedulerProvider(actorSystem::scheduler)
                                            .dispatcherProvider(actorSystem::dispatcher)
                                            .build();
        val control = new StatisticsRootActorControl(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(StatisticsEvents.DataTransferredEvent.class,
                                                    StatisticsEvents.ProjectionCreatedEvent.class,
                                                    StatisticsEvents.PCACreatedEvent.class,
                                                    StatisticsEvents.DimensionReductionCreatedEvent.class,
                                                    StatisticsEvents.IntersectionsCreatedEvent.class,
                                                    StatisticsEvents.NodesExtractedEvent.class,
                                                    StatisticsEvents.EdgePartitionCreatedEvent.class,
                                                    StatisticsEvents.PathScoresCreatedEvent.class,
                                                    StatisticsEvents.PathScoresNormalizedEvent.class,
                                                    StatisticsEvents.ResultsPersistedEvent.class,
                                                    StatisticsEvents.MachineUtilizationEvent.class,
                                                    StatisticsEvents.MessageExchangeUtilizationEvent.class,
                                                    StatisticsEvents.ActorPoolUtilizationEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
