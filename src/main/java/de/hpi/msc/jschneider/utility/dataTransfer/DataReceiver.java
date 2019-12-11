package de.hpi.msc.jschneider.utility.dataTransfer;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import lombok.Getter;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

public class DataReceiver
{
    private static final Logger Log = LogManager.getLogger(DataReceiver.class);

    @Getter
    private final UUID operationId;
    private final ProtocolParticipantControl<? extends ProtocolParticipantModel> control;
    @Getter
    private final Set<DataSink> dataSinks = new HashSet<>();
    @Getter
    private boolean hasFinished = false;
    private Consumer<DataReceiver> whenFinished;
    private Consumer<DataTransferMessages.DataPartMessage> onReceive;

    public DataReceiver(UUID operationId, ProtocolParticipantControl<? extends ProtocolParticipantModel> control)
    {
        this.operationId = operationId;
        this.control = control;
    }

    public DataReceiver addSink(DataSink sink)
    {
        dataSinks.add(sink);
        return this;
    }

    public DataReceiver whenFinished(Consumer<DataReceiver> callback)
    {
        whenFinished = callback;
        return this;
    }

    public DataReceiver onReceive(Consumer<DataTransferMessages.DataPartMessage> callback)
    {
        onReceive = callback;
        return this;
    }

    public void pull(ActorRef distributor)
    {
        if (hasFinished)
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
        for (val sink : dataSinks)
        {
            sink.write(message.getPart());

            if (message.isLastPart())
            {
                sink.close();
            }
        }

        invokeOnReceive(message);

        if (!message.isLastPart())
        {
            pull(message.getSender());
        }
        else if (!hasFinished)
        {
            hasFinished = true;
            invokeWhenFinished();
        }
    }

    private void invokeOnReceive(DataTransferMessages.DataPartMessage message)
    {
        if (onReceive == null)
        {
            return;
        }
        try
        {
            onReceive.accept(message);
        }
        catch (Exception exception)
        {
            Log.error(String.format("[%1$s] Unable to invoke on receive!", control.getClass().getName()), exception);
        }
    }

    private void invokeWhenFinished()
    {
        if (whenFinished == null)
        {
            return;
        }

        try
        {
            whenFinished.accept(this);
        }
        catch (Exception exception)
        {
            Log.error(String.format("[%1$s] Unable to invoke when finished!", control.getClass().getName()), exception);
        }
    }
}
