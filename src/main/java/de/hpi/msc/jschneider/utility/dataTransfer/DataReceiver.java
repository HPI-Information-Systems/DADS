package de.hpi.msc.jschneider.utility.dataTransfer;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.event.EventHandler;
import de.hpi.msc.jschneider.utility.event.EventImpl;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

public class DataReceiver
{
    @Getter
    private final String operationId;
    private final ProtocolParticipantControl<? extends ProtocolParticipantModel> control;
    @Getter
    private final Set<DataSink> dataSinks = new HashSet<>();
    @Getter
    private boolean finished = false;
    @Getter @Setter
    private Object state;
    private final EventImpl<DataReceiver> onFinished = new EventImpl<>();
    private final EventImpl<DataTransferMessages.DataPartMessage> onDataPartReceived = new EventImpl<>();
    private final DataTransferMessages.InitializeDataTransferMessage initializationMessage;
    private final LocalDateTime startTime = LocalDateTime.now();
    private LocalDateTime endTime;
    private final Counter transferredBytes = new Counter(0L);

    public DataReceiver(String operationId, DataTransferMessages.InitializeDataTransferMessage initializationMessage, ProtocolParticipantControl<? extends ProtocolParticipantModel> control)
    {
        this.operationId = operationId;
        this.initializationMessage = initializationMessage;
        this.control = control;
    }

    public DataReceiver addSink(DataSink sink)
    {
        dataSinks.add(sink);
        return this;
    }

    public DataReceiver whenFinished(EventHandler<DataReceiver> handler)
    {
        onFinished.subscribe(handler);
        return this;
    }

    public DataReceiver whenDataPartReceived(EventHandler<DataTransferMessages.DataPartMessage> handler)
    {
        onDataPartReceived.subscribe(handler);
        return this;
    }

    public void pull(ActorRef distributor)
    {
        if (finished)
        {
            return;
        }

        val message = DataTransferMessages.RequestNextDataPartMessage.builder()
                                                                     .sender(control.getModel().getSelf())
                                                                     .receiver(distributor)
                                                                     .operationId(operationId)
                                                                     .build();
        control.send(message);
    }

    public void receive(DataTransferMessages.DataPartMessage message)
    {
        transferredBytes.increment(message.getPart().length);

        for (val sink : dataSinks)
        {
            sink.write(message.getPart());

            if (message.isLastPart())
            {
                sink.close();
            }
        }

        onDataPartReceived.invoke(message);

        if (!message.isLastPart())
        {
            pull(message.getSender());
        }
        else if (!finished)
        {
            endTime = LocalDateTime.now();
            finished = true;
            onFinished.invoke(this);

            control.trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.DataTransferCompletedEvent.builder()
                                                                                                                        .sender(control.getModel().getSelf())
                                                                                                                        .receiver(eventDispatcher)
                                                                                                                        .processor(ProcessorId.of(control.getModel().getSelf()))
                                                                                                                        .startTime(startTime)
                                                                                                                        .endTime(endTime)
                                                                                                                        .initializationMessageType(initializationMessage.getClass())
                                                                                                                        .source(ProcessorId.of(message.getSender()))
                                                                                                                        .sink(ProcessorId.of(control.getModel().getSelf()))
                                                                                                                        .transferredBytes(transferredBytes.get())
                                                                                                                        .build());
        }
    }
}
