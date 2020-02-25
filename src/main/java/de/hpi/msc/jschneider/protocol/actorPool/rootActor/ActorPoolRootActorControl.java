package de.hpi.msc.jschneider.protocol.actorPool.rootActor;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerModel;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

public class ActorPoolRootActorControl extends AbstractProtocolParticipantControl<ActorPoolRootActorModel>
{
    public ActorPoolRootActorControl(ActorPoolRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(ActorPoolMessages.WorkMessage.class, this::onWork)
                    .match(ActorPoolMessages.WorkDoneMessage.class, this::onWorkDone);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        for (var workerNumber = 0; workerNumber < getModel().getMaximumNumberOfWorkers(); ++workerNumber)
        {
            spawnWorker();
        }
    }

    private void spawnWorker()
    {
        val model = ActorPoolWorkerModel.builder()
                                        .supervisor(getModel().getSelf())
                                        .build();
        val control = new ActorPoolWorkerControl(model);
        val worker = trySpawnChild(ProtocolParticipant.props(control), "ActorPoolWorker");

        if (!worker.isPresent())
        {
            getLog().error("Unable to spawn new ActorPoolWorker!");
            return;
        }

        getModel().getWorkers().add(worker.get());
    }

    private void onWork(ActorPoolMessages.WorkMessage message)
    {
        try
        {
            if (!getModel().getWorkQueue().add(message))
            {
                getLog().error(String.format("Unable to enqueue work message from %1$s!", message.getSender().path()));
                return;
            }
            workOnNextItem();
        }
        finally
        {
            complete(message);
        }
    }

    private void onWorkDone(ActorPoolMessages.WorkDoneMessage message)
    {
        try
        {
            getModel().getWorkers().add(message.getSender());
            workOnNextItem();
        }
        finally
        {
            complete(message);
        }
    }

    private void workOnNextItem()
    {
        if (getModel().getWorkers().isEmpty())
        {
            return;
        }

        if (getModel().getWorkQueue().isEmpty())
        {
            return;
        }

        val worker = getModel().getWorkers().poll();
        val work = getModel().getWorkQueue().poll();
        send(work.redirectTo(worker));
    }
}
