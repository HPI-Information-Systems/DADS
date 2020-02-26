package de.hpi.msc.jschneider.utility.dataTransfer.distributor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;

import java.time.LocalDateTime;

public class DataDistributorControl extends AbstractProtocolParticipantControl<DataDistributorModel>
{
    public static final double MESSAGE_SIZE_FACTOR = 0.25d;


    public DataDistributorControl(DataDistributorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(DataTransferMessages.RequestNextDataPartMessage.class, this::onRequestNextDataPart);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        val initializationMessage = getModel().getInitializer().initialize(getModel().getSelf(), getModel().getOperationId());
        send(initializationMessage);

        getModel().setInitializationMessage(initializationMessage);
        getModel().setStartTime(LocalDateTime.now());
    }

    private void onRequestNextDataPart(DataTransferMessages.RequestNextDataPartMessage message)
    {
        try
        {
            transfer(message.getSender());
        }
        finally
        {
            complete(message);
        }
    }

    private void transfer(ActorRef receiver)
    {
        if (getModel().isFinished())
        {
            return;
        }

        val data = getModel().getDataSource().read((int) (getModel().getMaximumMessageSize() * MESSAGE_SIZE_FACTOR));
        getModel().getTransferredBytes().increment(data.length);

        val message = DataTransferMessages.DataPartMessage.builder()
                                                          .sender(getModel().getSelf())
                                                          .receiver(receiver)
                                                          .part(data)
                                                          .isLastPart(getModel().getDataSource().isAtEnd())
                                                          .operationId(getModel().getOperationId())
                                                          .build();
        send(message);

        if (getModel().getDataSource().isAtEnd() && !getModel().isFinished())
        {
            getModel().setFinished(true);
            getModel().setEndTime(LocalDateTime.now());

            send(DataTransferMessages.DataTransferFinishedMessage.builder()
                                                                 .sender(getModel().getSelf())
                                                                 .receiver(getModel().getSupervisor())
                                                                 .operationId(getModel().getOperationId())
                                                                 .build());

            trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.DataTransferCompletedEvent.builder()
                                                                                                                .sender(getModel().getSelf())
                                                                                                                .receiver(eventDispatcher)
                                                                                                                .processor(ProcessorId.of(getModel().getSelf()))
                                                                                                                .initializationMessageClassName(getModel().getInitializationMessage().getClass().getName())
                                                                                                                .source(ProcessorId.of(getModel().getSelf()))
                                                                                                                .sink(ProcessorId.of(getModel().getInitializationMessage().getReceiver()))
                                                                                                                .startTime(getModel().getStartTime())
                                                                                                                .endTime(getModel().getEndTime())
                                                                                                                .transferredBytes(getModel().getTransferredBytes().get())
                                                                                                                .build());
        }
    }
}
