package de.hpi.msc.jschneider.protocol.heartbeat.rootActor;

import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.heartbeat.HeartbeatEvents;
import de.hpi.msc.jschneider.protocol.heartbeat.HeartbeatProtocol;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.time.Duration;
import java.time.LocalDateTime;

public class HeartbeatRootActorControl extends AbstractProtocolParticipantControl<HeartbeatRootActorModel>
{
    public HeartbeatRootActorControl(HeartbeatRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetup)
                    .match(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class, this::onRegistrationAcknowledged)
                    .match(HeartbeatEvents.StillAliveEvent.class, this::onStillAlive);
    }

    private void onSetup(CommonMessages.SetUpProtocolMessage message)
    {
        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToRemoteEvents();
            startSendingHeartbeats();
        }
        finally
        {
            complete(message);
        }
    }

    private void subscribeToRemoteEvents()
    {
        for (val participant : getModel().getProcessors())
        {
            val protocol = getProtocol(participant.getId(), ProtocolType.Heartbeat);
            if (!protocol.isPresent())
            {
                continue;
            }

            getModel().getExpectedHeartbeats().put(participant.getId(), LocalDateTime.now().plus(HeartbeatProtocol.HEARTBEAT_INTERVAL));
            subscribeToEvent(participant.getId(), ProtocolType.Heartbeat, HeartbeatEvents.StillAliveEvent.class);
        }
    }

    private void startSendingHeartbeats()
    {
        if (getModel().getSendHeartbeatsTask() != null)
        {
            getModel().getSendHeartbeatsTask().cancel();
        }

        if (getModel().getCheckProcessorsStateTask() != null)
        {
            getModel().getCheckProcessorsStateTask().cancel();
        }

        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        assert scheduler != null : "Scheduler must not be null!";
        assert dispatcher != null : "Dispatcher must not be null!";

        val sendTask = scheduler.scheduleAtFixedRate(Duration.ZERO,
                                                     HeartbeatProtocol.HEARTBEAT_INTERVAL,
                                                     this::sendHeartbeat,
                                                     dispatcher);
        getModel().setSendHeartbeatsTask(sendTask);

        val checkTask = scheduler.scheduleAtFixedRate(Duration.ZERO,
                                                      HeartbeatProtocol.HEARTBEAT_CHECK_INTERVAL,
                                                      this::checkProcessorState,
                                                      dispatcher);
        getModel().setCheckProcessorsStateTask(checkTask);
    }

    private void sendHeartbeat()
    {
        getLog().debug("Sending heartbeat...");
        trySendEvent(ProtocolType.Heartbeat, eventDispatcher -> HeartbeatEvents.StillAliveEvent.builder()
                                                                                               .sender(getModel().getSelf())
                                                                                               .receiver(eventDispatcher)
                                                                                               .build());
    }

    private void checkProcessorState()
    {
        val now = LocalDateTime.now();
        for (val participant : getModel().getExpectedHeartbeats().keySet())
        {
            val nextHeartbeat = getModel().getExpectedHeartbeats().get(participant);
            if (nextHeartbeat.isAfter(now))
            {
                continue;
            }

            val diff = Duration.between(nextHeartbeat, now);
            if (diff.minus(HeartbeatProtocol.HEARTBEAT_WARNING_INTERVAL).isNegative())
            {
                continue;
            }

            if (diff.minus(HeartbeatProtocol.HEARTBEAT_PANIC_INTERVAL).isNegative())
            {
                getLog().warn("{} is {} too late on sending a heartbeat!", participant, diff);
                continue;
            }

            getLog().error("{} does not send heartbeats anymore!", participant);
            trySendEvent(ProtocolType.Heartbeat, eventDispatcher -> HeartbeatEvents.HeartbeatPanicEvent.builder()
                                                                                                       .sender(getModel().getSelf())
                                                                                                       .receiver(eventDispatcher)
                                                                                                       .build());
            break;
        }
    }

    private void onStillAlive(HeartbeatEvents.StillAliveEvent message)
    {
        try
        {
            getLog().debug("Received heartbeat from {}.", ProcessorId.of(message.getSender()));
            getModel().getExpectedHeartbeats().put(ProcessorId.of(message.getSender()), LocalDateTime.now().plus(HeartbeatProtocol.HEARTBEAT_INTERVAL));
        }
        finally
        {
            complete(message);
        }
    }
}
