package de.hpi.msc.jschneider.utility.dataTransfer;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.IdGenerator;
import de.hpi.msc.jschneider.utility.dataTransfer.distributor.DataDistributorControl;
import de.hpi.msc.jschneider.utility.dataTransfer.distributor.DataDistributorInitializer;
import de.hpi.msc.jschneider.utility.dataTransfer.distributor.DataDistributorModel;
import de.hpi.msc.jschneider.utility.event.EventHandler;
import de.hpi.msc.jschneider.utility.event.EventImpl;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class DataTransferManager
{
    private static final Logger Log = LogManager.getLogger(DataTransferManager.class);

    private final Map<Long, DataReceiver> dataReceivers = new HashMap<>();
    private final Map<Long, ActorRef> dataDistributors = new HashMap<>();
    private final ProtocolParticipantControl<? extends ProtocolParticipantModel> control;
    private final EventImpl<Long> onFinish = new EventImpl<>();

    public DataTransferManager(ProtocolParticipantControl<? extends ProtocolParticipantModel> control)
    {
        this.control = control;
    }

    public boolean hasRunningDataTransfers()
    {
        return !dataDistributors.isEmpty() || !dataReceivers.isEmpty();
    }

    public DataTransferManager whenFinished(EventHandler<Long> handler)
    {
        onFinish.subscribe(handler);
        return this;
    }

    public void transfer(DataSource dataSource, DataDistributorInitializer initializer)
    {
        val operationId = IdGenerator.next();
        val model = DataDistributorModel.builder()
                                        .operationId(operationId)
                                        .supervisor(control.getModel().getSelf())
                                        .dataSource(dataSource)
                                        .initializer(initializer)
                                        .build();
        val distributorControl = new DataDistributorControl(model);
        val distributor = control.trySpawnChild(distributorControl, "DataDistributor");

        if (!distributor.isPresent())
        {
            Log.error("Unable to create new DataDistributor!");
            return;
        }

        dataDistributors.put(operationId, distributor.get());
    }

    public void accept(DataTransferMessages.InitializeDataTransferMessage initializationMessage, Function<DataReceiver, DataReceiver> dataReceiverInitializer)
    {
        try
        {
            var receiver = dataReceivers.get(initializationMessage.getOperationId());
            if (receiver != null)
            {
                Log.error("[{}] A data transfer for the operation id {} has already been accepted!",
                          control.getClass().getName(),
                          initializationMessage.getOperationId());
                return;
            }

            receiver = dataReceiverInitializer.apply(new DataReceiver(initializationMessage.getOperationId(), initializationMessage, control));
            receiver.whenFinished(this::whenDataReceived);
            dataReceivers.put(receiver.getOperationId(), receiver);

            receiver.requestSynchronization(initializationMessage.getSender());
        }
        finally
        {
            control.complete(initializationMessage);
        }
    }

    public void onSynchronization(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        try
        {
            val receiver = dataReceivers.get(message.getOperationId());
            if (receiver == null)
            {
                Log.error("[{}] Unable to process data part, because there is no receiver for that operation!", control.getClass().getName());
                return;
            }

            receiver.synchronize(message);
        }
        finally
        {
            control.complete(message);
        }
    }

    public void onPart(DataTransferMessages.DataPartMessage message)
    {
        try
        {
            val receiver = dataReceivers.get(message.getOperationId());
            if (receiver == null)
            {
                Log.error("[{}] Unable to process data part, because there is no receiver for that operation!", control.getClass().getName());
                return;
            }

            receiver.receive(message);
        }
        finally
        {
            control.complete(message);
        }
    }

    private void whenDataReceived(DataReceiver receiver)
    {
        dataReceivers.remove(receiver.getOperationId());

        onFinish.invoke(receiver.getOperationId());
    }

    public void onDataSent(DataTransferMessages.DataTransferFinishedMessage message)
    {
        try
        {
            dataDistributors.remove(message.getOperationId());
            onFinish.invoke(message.getOperationId());
        }
        finally
        {
            control.complete(message);
        }
    }
}
