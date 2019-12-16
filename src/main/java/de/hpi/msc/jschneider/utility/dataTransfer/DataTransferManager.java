package de.hpi.msc.jschneider.utility.dataTransfer;

import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.dataTransfer.source.PrimitiveAccessSource;
import lombok.Getter;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ojalgo.structure.Access1D;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class DataTransferManager
{
    private static final Logger Log = LogManager.getLogger(DataTransferManager.class);

    @Getter
    private final Map<UUID, DataDistributor> dataDistributors = new HashMap<>();
    @Getter
    private final Map<UUID, DataReceiver> dataReceivers = new HashMap<>();
    private final ProtocolParticipantControl<? extends ProtocolParticipantModel> control;

    public DataTransferManager(ProtocolParticipantControl<? extends ProtocolParticipantModel> control)
    {
        this.control = control;
    }

    public void transfer(Access1D<Double> data, Function<DataDistributor, DataTransferMessages.InitializeDataTransferMessage> initializationMessageFactory)
    {
        transfer(data, distributor -> distributor, initializationMessageFactory);
    }

    public void transfer(Access1D<Double> data, Function<DataDistributor, DataDistributor> dataDistributorInitializer, Function<DataDistributor, DataTransferMessages.InitializeDataTransferMessage> initializationMessageFactory)
    {
        transfer(new PrimitiveAccessSource(data), dataDistributorInitializer, initializationMessageFactory);
    }

    public void transfer(DataSource dataSource, Function<DataDistributor, DataTransferMessages.InitializeDataTransferMessage> initializationMessageFactory)
    {
        transfer(dataSource, distributor -> distributor, initializationMessageFactory);
    }

    public void transfer(DataSource dataSource, Function<DataDistributor, DataDistributor> dataDistributorInitializer, Function<DataDistributor, DataTransferMessages.InitializeDataTransferMessage> initializationMessageFactory)
    {
        val distributor = dataDistributorInitializer.apply(new DataDistributor(control, dataSource));
        dataDistributors.put(distributor.getOperationId(), distributor);

        distributor.initialize(initializationMessageFactory);
    }

    public void accept(DataTransferMessages.InitializeDataTransferMessage initializationMessage, Function<DataReceiver, DataReceiver> dataReceiverInitializer)
    {
        try
        {
            var receiver = dataReceivers.get(initializationMessage.getOperationId());
            if (receiver != null)
            {
                Log.error(String.format("[%1$s] A data transfer for the operation id %2$s has already been accepted!",
                                        control.getClass().getName(),
                                        initializationMessage.getOperationId()));
                return;
            }

            receiver = dataReceiverInitializer.apply(new DataReceiver(initializationMessage.getOperationId(), control));
            dataReceivers.put(receiver.getOperationId(), receiver);

            receiver.pull(initializationMessage.getSender());
        }
        finally
        {
            control.complete(initializationMessage);
        }
    }

    public void onRequestNextPart(DataTransferMessages.RequestNextDataPartMessage message)
    {
        try
        {
            val distributor = dataDistributors.get(message.getOperationId());
            if (distributor == null)
            {
                Log.error(String.format("[$1%s] Unable to deliver next data part, because there is no distributor for that operation!",
                                        control.getClass().getName()));
                return;
            }

            distributor.transfer(message.getSender());
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
                Log.error(String.format("[%1$s] Unable to process data part, because there is no receiver for that operation!", control.getClass().getName()));
                return;
            }

            receiver.receive(message);
        }
        finally
        {
            control.complete(message);
        }
    }
}
