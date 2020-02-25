package de.hpi.msc.jschneider.protocol.actorPool.worker;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.time.Duration;
import java.time.LocalDateTime;

public class ActorPoolWorkerControl extends AbstractProtocolParticipantControl<ActorPoolWorkerModel>
{
    public ActorPoolWorkerControl(ActorPoolWorkerModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(ActorPoolMessages.WorkMessage.class, this::onWork);
    }

    private void onWork(ActorPoolMessages.WorkMessage message)
    {
        val startTime = LocalDateTime.now();
        try
        {
            getLog().info(String.format("PoolWorker (%1$s) starts working on %2$s.",
                                        getModel().getSelf().path(),
                                        message.getClass().getSimpleName()));

            message.getConsumer().process(this, message);
        }
        finally
        {
            val endTime = LocalDateTime.now();

            getLog().info(String.format("PoolWorker (%1$s) finished working on %2$s after %3$s (supervisor = %4$s).",
                                        getModel().getSelf().path(),
                                        message.getClass().getSimpleName(),
                                        Duration.between(startTime, endTime),
                                        getModel().getSupervisor().path()));

            send(ActorPoolMessages.WorkDoneMessage.builder()
                                                  .sender(getModel().getSelf())
                                                  .receiver(getModel().getSupervisor())
                                                  .build());
            complete(message);
        }
    }
}
