package de.hpi.msc.jschneider.utility.dataTransfer;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.event.EventHandler;
import de.hpi.msc.jschneider.utility.event.EventImpl;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.function.Function;

public class DataDistributor
{
    public static final double MESSAGE_SIZE_FACTOR = 0.75d;

    private static final Logger Log = LogManager.getLogger(DataDistributor.class);

    @Getter
    private final String operationId = UUID.randomUUID().toString();
    private final ProtocolParticipantControl<? extends ProtocolParticipantModel> control;
    private final DataSource dataSource;
    @Getter
    private boolean initialized = false;
    @Getter
    private boolean finished = false;
    @Getter @Setter
    private Object state;
    private final EventImpl<DataDistributor> onFinished = new EventImpl<>();

    public DataDistributor(ProtocolParticipantControl<? extends ProtocolParticipantModel> control, DataSource dataSource)
    {
        this.control = control;
        this.dataSource = dataSource;
    }

    public DataDistributor whenFinished(EventHandler<DataDistributor> handler)
    {
        onFinished.subscribe(handler);
        return this;
    }

    public void initialize(Function<DataDistributor, DataTransferMessages.InitializeDataTransferMessage> initializationMessageFactory)
    {
        if (initialized)
        {
            Log.error(String.format("[%1$s] Data transfer has already been initializer!", control.getClass().getName()));
            return;
        }

        try
        {
            val initializationMessage = initializationMessageFactory.apply(this);
            control.send(initializationMessage);

            initialized = true;
        }
        catch (Exception exception)
        {
            Log.error(String.format("[%1$s] Unable to initialize data transfer!", control.getClass().getName()), exception);
        }
    }

    public void transfer(ActorRef receiver)
    {
        if (finished)
        {
            return;
        }

        val data = dataSource.read((int) (control.getModel().getMaximumMessageSize() * MESSAGE_SIZE_FACTOR));
        val message = DataTransferMessages.DataPartMessage.builder()
                                                          .sender(control.getModel().getSelf())
                                                          .receiver(receiver)
                                                          .part(data)
                                                          .isLastPart(dataSource.isAtEnd())
                                                          .operationId(operationId)
                                                          .build();
        control.send(message);

        Log.info(String.format("[$1%s] Sending data part (size = %2$d, isLast = %3$s) to %4$s.",
                               control.getClass().getName(),
                               data.length,
                               dataSource.isAtEnd(),
                               receiver.path()));

        if (dataSource.isAtEnd() && !finished)
        {
            finished = true;
            Log.info(String.format("[%1$s] Done sending data to %2$s.", control.getClass().getName(), receiver.path()));
            onFinished.invoke(this);
        }
    }
}
