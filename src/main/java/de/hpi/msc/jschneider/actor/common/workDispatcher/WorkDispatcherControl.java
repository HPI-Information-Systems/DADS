package de.hpi.msc.jschneider.actor.common.workDispatcher;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.master.nodeRegistry.NodeRegistry;
import de.hpi.msc.jschneider.actor.master.nodeRegistry.NodeRegistryMessages;
import lombok.val;

import java.time.Duration;

public class WorkDispatcherControl extends AbstractActorControl<WorkDispatcherModel>
{
    public WorkDispatcherControl(WorkDispatcherModel model)
    {
        super(model);
    }

    public void onAcknowledgeRegistration(WorkDispatcherMessages.AcknowledgeRegistrationMessage message)
    {
        try
        {
            cancelRegistrationSchedule();
        }
        finally
        {
            complete(message);
        }
    }

    public void onRegisterAtMaster(WorkDispatcherMessages.RegisterAtMasterMessage message)
    {
        cancelRegistrationSchedule();
        val nodeRegistry = selectActor(String.format("%1$s/user/%2$s", message.getMasterAddress(), NodeRegistry.NAME));
        if (nodeRegistry == null)
        {
            return;
        }

        getModel().setRegisterSchedule(createRegistrationSchedule(nodeRegistry));
    }

    private void cancelRegistrationSchedule()
    {
        val schedule = getModel().getRegisterSchedule();
        if (schedule == null)
        {
            return;
        }

        schedule.cancel();
    }

    private ActorSelection selectActor(String address)
    {
        try
        {
            return getModel().getActorSelectionProvider().apply(address);
        }
        catch (Exception exception)
        {
            getLog().error(String.format("Unable to select actor(s) at \"%1$s\"!", address));
            return null;
        }
    }

    private Cancellable createRegistrationSchedule(ActorSelection nodeRegistry)
    {
        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        if (scheduler == null || dispatcher == null)
        {
            return null;
        }

        val message = NodeRegistryMessages.RegisterWorkerNodeMessage.builder()
                                                                    .sender(getSelf())
                                                                    .receiver(ActorRef.noSender())
                                                                    .messageDispatcher(getMessageDispatcher())
                                                                    .workDispatcher(getSelf())
                                                                    .maximumMemory(SystemParameters.getMaximumMemory())
                                                                    .numberOfWorkers(SystemParameters.getNumberOfWorkers())
                                                                    .build();

        return scheduler.scheduleAtFixedRate(Duration.ZERO, Duration.ofSeconds(5), () -> nodeRegistry.tell(message, getSelf()), dispatcher);
    }
}
