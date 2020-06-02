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
    public DataDistributorControl(DataDistributorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(DataTransferMessages.RequestDataTransferSynchronizationMessage.class, this::onRequestSynchronization)
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

    private void onRequestSynchronization(DataTransferMessages.RequestDataTransferSynchronizationMessage message)
    {
        try
        {
            send(getModel().getDataSource().createSynchronizationMessage(getModel().getSelf(), message.getSender(), getModel().getOperationId()));
        }
        finally
        {
            complete(message);
        }
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

        getModel().getDataSource().next();

        val buffer = getModel().getDataSource().buffer();
        val bufferLength = getModel().getDataSource().bufferLength();

        getModel().getTransferredBytes().increment(bufferLength);

        val message = DataTransferMessages.DataPartMessage.builder()
                                                          .sender(getModel().getSelf())
                                                          .receiver(receiver)
                                                          .part(buffer)
                                                          .partLength(bufferLength)
                                                          .isLastPart(!getModel().getDataSource().hasNext())
                                                          .operationId(getModel().getOperationId())
                                                          .build();
        send(message);

        if (!getModel().getDataSource().hasNext() && !getModel().isFinished())
        {
            getModel().setFinished(true);
            getModel().setEndTime(LocalDateTime.now());

            send(DataTransferMessages.DataTransferFinishedMessage.builder()
                                                                 .sender(getModel().getSelf())
                                                                 .receiver(getModel().getSupervisor())
                                                                 .operationId(getModel().getOperationId())
                                                                 .build());

            trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.DataTransferredEvent.builder()
                                                                                                          .sender(getModel().getSelf())
                                                                                                          .receiver(eventDispatcher)
                                                                                                          .initializationMessageClassName(getModel().getInitializationMessage().getClass().getName())
                                                                                                          .source(ProcessorId.of(getModel().getSelf()))
                                                                                                          .sink(ProcessorId.of(getModel().getInitializationMessage().getReceiver()))
                                                                                                          .startTime(getModel().getStartTime())
                                                                                                          .endTime(getModel().getEndTime())
                                                                                                          .transferredBytes(getModel().getTransferredBytes().get())
                                                                                                          .build());

            isReadyToBeTerminated();
        }
    }
}
