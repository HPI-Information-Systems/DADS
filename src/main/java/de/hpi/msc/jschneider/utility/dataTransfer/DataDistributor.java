package de.hpi.msc.jschneider.utility.dataTransfer;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import lombok.Getter;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataDistributor
{
    public static final float MESSAGE_SIZE_FACTOR = 0.75f;

    private static final Logger Log = LogManager.getLogger(DataDistributor.class);

    @Getter
    private final UUID operationId = UUID.randomUUID();
    private final ProtocolParticipantControl<? extends ProtocolParticipantModel> control;
    private final DataSource dataSource;
    @Getter
    private boolean hasBeenInitialized = false;
    @Getter
    private boolean hasFinished = false;
    private Consumer<DataDistributor> whenFinished;

    public DataDistributor(ProtocolParticipantControl<? extends ProtocolParticipantModel> control, DataSource dataSource)
    {
        this.control = control;
        this.dataSource = dataSource;
    }

    public DataDistributor whenFinished(Consumer<DataDistributor> callback)
    {
        whenFinished = callback;
        return this;
    }

    public void initialize(Function<DataDistributor, DataTransferMessages.InitializeDataTransferMessage> initializationMessageFactory)
    {
        if (hasBeenInitialized)
        {
            Log.error(String.format("[%1$s] Data transfer has already been initializer!", control.getClass().getName()));
            return;
        }

        try
        {
            val initializationMessage = initializationMessageFactory.apply(this);
            control.send(initializationMessage);

            hasBeenInitialized = true;
        }
        catch (Exception exception)
        {
            Log.error(String.format("[%1$s] Unable to initialize data transfer!", control.getClass().getName()), exception);
        }
    }

    public void transfer(ActorRef receiver)
    {
        if (hasFinished)
        {
            return;
        }

        val data = dataSource.read((long) (control.getModel().getMaximumMessageSize() / Float.BYTES * MESSAGE_SIZE_FACTOR));
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

        if (dataSource.isAtEnd() && !hasFinished)
        {
            hasFinished = true;
            Log.info(String.format("[%1$s] Done sending data to %2$s.", control.getClass().getName(), receiver.path()));
            invokeWhenFinished();
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
