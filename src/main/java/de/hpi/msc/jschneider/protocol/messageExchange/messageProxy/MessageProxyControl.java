package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;

public class MessageProxyControl extends AbstractProtocolParticipantControl<MessageProxyModel>
{
    private static class MeasureUtilization implements Serializable
    {
        private static final long serialVersionUID = 1718695408584964708L;
    }

    public MessageProxyControl(MessageProxyModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class, this::onRegistrationAcknowledged)
                      .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination)
                      .match(MessageExchangeMessages.MessageCompletedMessage.class, this::onMessageCompleted)
                      .match(MessageExchangeMessages.MessageExchangeMessage.class, this::onMessage)
                      .match(MeasureUtilization.class, this::measureUtilization);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        if (StatisticsProtocol.IS_ENABLED)
        {
            if (getModel().getNumberOfProcessors() <= 1)
            {
                subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
            }
            else
            {
                initializeUtilizationMeasurements();
            }
        }
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        if (!message.getReceiver().path().equals(getModel().getSelf().path()))
        {
            // if we are not the intended receiver, we need to forward the message accordingly
            onMessage(message);
            return;
        }

        initializeUtilizationMeasurements();
    }

    private void initializeUtilizationMeasurements()
    {
        subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);
        startMeasureUtilization();
    }

    private void startMeasureUtilization()
    {
        if (getModel().getMeasureUtilizationTask() != null)
        {
            return;
        }

        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        assert scheduler != null : "Scheduler must not be null!";
        assert dispatcher != null : "Dispatcher must not be null!";

        val task = scheduler.scheduleAtFixedRate(Duration.ZERO,
                                                 StatisticsProtocol.MEASUREMENT_INTERVAL,
                                                 () -> getModel().getSelf().tell(new MeasureUtilization(), getModel().getSelf()),
                                                 dispatcher);
        getModel().setMeasureUtilizationTask(task);
    }

    private void measureUtilization(MeasureUtilization message)
    {
        if (getModel().getMessageQueues().size() < 1)
        {
            return;
        }

        val totalEnqueuedMessages = new Counter(0L);
        val totalUnacknowledgedMessages = new Counter(0L);
        var largestMessageQueueSize = 0L;
        ActorPath largestMessageQueueReceiver = null;

        for (val entry : getModel().getMessageQueues().entrySet())
        {
            val receiver = entry.getKey();
            val queue = entry.getValue();

            totalEnqueuedMessages.increment(queue.size());
            totalUnacknowledgedMessages.increment(queue.numberOfUncompletedMessages());

            if (queue.size() <= largestMessageQueueSize && largestMessageQueueReceiver != null)
            {
                continue;
            }

            largestMessageQueueSize = queue.size();
            largestMessageQueueReceiver = receiver;
        }

        val averageQueueSize = totalEnqueuedMessages.get() / (double) getModel().getMessageQueues().size();

        val finalLargestMessageQueueSize = largestMessageQueueSize;
        val finalLargestMessageQueueReceiver = largestMessageQueueReceiver;

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.MessageExchangeUtilizationEvent.builder()
                                                                                                                 .sender(getModel().getSelf())
                                                                                                                 .receiver(eventDispatcher)
                                                                                                                 .dateTime(LocalDateTime.now())
                                                                                                                 .remoteProcessor(ProcessorId.of(getModel().getRemoteMessageDispatcher()))
                                                                                                                 .totalNumberOfEnqueuedMessages(totalEnqueuedMessages.get())
                                                                                                                 .totalNumberOfUnacknowledgedMessages(totalUnacknowledgedMessages.get())
                                                                                                                 .largestMessageQueueSize(finalLargestMessageQueueSize)
                                                                                                                 .largestMessageQueueReceiver(finalLargestMessageQueueReceiver)
                                                                                                                 .averageMessageQueueSize(averageQueueSize)
                                                                                                                 .build());
    }

    private void onReadyForTermination(ScoringEvents.ReadyForTerminationEvent message)
    {
        if (!message.getReceiver().path().equals(getModel().getSelf().path()))
        {
            // if we are not the intended receiver, we need to forward the message accordingly
            onMessage(message);
            return;
        }

        if (getModel().getMeasureUtilizationTask() != null)
        {
            getModel().getMeasureUtilizationTask().cancel();
        }
    }

    private void onMessageCompleted(MessageExchangeMessages.MessageCompletedMessage message)
    {
        val senderQueue = getModel().getMessageQueues().get(message.getSender().path());
        if (senderQueue == null)
        {
            getLog().error("Unable to get message queue for {} in order to complete earlier message!",
                           message.getSender().path());
            return;
        }
        else if (!senderQueue.tryAcknowledge(message.getCompletedMessageId()))
        {
            getLog().error("Unexpected message completion for a message we have never seen before! (sender = {}, receiver = {})",
                           message.getSender().path(),
                           message.getReceiver().path());
            return;
        }

        getModel().getTotalNumberOfEnqueuedMessages().decrement();
        dequeueAndSend(senderQueue);

        if (!ProcessorId.of(message.getReceiver()).equals(ProcessorId.of(getModel().getSelf())))
        {
            forward(message);
        }
    }

    private void onMessage(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val receiverQueue = getOrCreateMessageQueue(message.getReceiver().path());
        val receiverQueueSize = receiverQueue.enqueueBack(message);
        val totalQueueSize = getModel().getTotalNumberOfEnqueuedMessages().incrementAndGet();

        if (receiverQueueSize == 1)
        {
            dequeueAndSend(receiverQueue);
        }

        if (receiverQueueSize < getModel().getSingleQueueBackPressureThreshold() &&
            totalQueueSize < getModel().getTotalQueueBackPressureThreshold())
        {
            return;
        }

        applyBackPressure(message.getSender());
    }

    private ActorMessageQueue getOrCreateMessageQueue(ActorPath receiverPath)
    {
        var queue = getModel().getMessageQueues().get(receiverPath);
        if (queue == null)
        {
            queue = new ActorMessageQueue();
            getModel().getMessageQueues().put(receiverPath, queue);
        }

        return queue;
    }

    private void dequeueAndSend(ActorMessageQueue queue)
    {
        if (queue == null)
        {
            return;
        }

        val message = queue.dequeue();
        if (message == null)
        {
            return;
        }

        getModel().getTotalNumberOfEnqueuedMessages().decrement();
        forward(message);
    }

    private void forward(MessageExchangeMessages.MessageExchangeMessage message)
    {
        if (ProcessorId.of(message.getReceiver()).equals(ProcessorId.of(getModel().getSelf())))
        {
            message.getReceiver().tell(message, message.getSender());
        }
        else
        {
            getModel().getRemoteMessageDispatcher().tell(message, message.getSender());
        }
    }

    private void applyBackPressure(ActorRef receiver)
    {
//        if (!ProcessorId.of(receiver).equals(ProcessorId.of(getModel().getSelf())))
//        {
//            // apply back pressure only to local actors
//            return;
//        }
//
//        val queue = getOrCreateMessageQueue(receiver.path());
//        val message = MessageExchangeMessages.BackPressureMessage.builder()
//                                                                 .sender(getModel().getSelf())
//                                                                 .receiver(receiver)
//                                                                 .build();
//        val queueSize = queue.enqueueFront(message);
//        getModel().getTotalNumberOfEnqueuedMessages().increment();
//
//        if (queueSize == 1)
//        {
//            dequeueAndSend(queue);
//        }
    }
}
