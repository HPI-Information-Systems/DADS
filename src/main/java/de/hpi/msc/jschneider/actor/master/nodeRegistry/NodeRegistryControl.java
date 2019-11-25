package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcherMessages;
import lombok.val;

public class NodeRegistryControl extends AbstractActorControl<NodeRegistryModel>
{
    public NodeRegistryControl(NodeRegistryModel model)
    {
        super(model);
        getModel().getWorkerNodes().put(getSelf().path().root(), WorkerNode.builder()
                                                                           .messageDispatcher(getMessageDispatcher())
                                                                           .workDispatcher(getModel().getWorkDispatcher())
                                                                           .maximumMemory(SystemParameters.getMaximumMemory())
                                                                           .numberOfWorkers(SystemParameters.getNumberOfWorkers())
                                                                           .build());
    }

    public void onRegistration(NodeRegistryMessages.RegisterWorkerNodeMessage message)
    {
        val rootPath = message.getMessageDispatcher().path().root();
        getModel().getWorkerNodes().put(rootPath, WorkerNode.fromRegistrationMessage(message));

        introduceMessageDispatcher(message.getMessageDispatcher());

        getLog().info(String.format("%1$s just registered with %2$d available workers.", rootPath, message.getNumberOfWorkers()));
    }

    private void introduceMessageDispatcher(ActorRef messageDispatcher)
    {
        val rootPath = messageDispatcher.path().root();

        for (val registrationMessage : getModel().getWorkerNodes().values())
        {
            if (registrationMessage.getMessageDispatcher().path().root() == rootPath)
            {
                continue;
            }

            if (getSelf().path().root() == rootPath)
            {
                continue;
            }

            introduceNewMessageDispatcherToExistingMessageDispatcher(messageDispatcher, registrationMessage.getMessageDispatcher());
        }

        introduceExistingMessageDispatchersToNewMessageDispatcher(messageDispatcher);
    }

    private void introduceNewMessageDispatcherToExistingMessageDispatcher(ActorRef newMessageDispatcher, ActorRef existingMessageDispatcher)
    {
        send(MessageDispatcherMessages.AddMessageDispatchersMessage.builder()
                                                                   .sender(newMessageDispatcher)
                                                                   .receiver(existingMessageDispatcher)
                                                                   .messageDispatchers(new ActorRef[]{newMessageDispatcher})
                                                                   .build());
    }

    private void introduceExistingMessageDispatchersToNewMessageDispatcher(ActorRef messageDispatcher)
    {
        val rootPath = messageDispatcher.path().root();
        val existingDispatchers = getModel().getWorkerNodes().entrySet()
                                            .stream()
                                            .filter(keyValuePair -> keyValuePair.getKey() != rootPath)
                                            .map(keyValuePair -> keyValuePair.getValue().getMessageDispatcher())
                                            .toArray(ActorRef[]::new);

        if (existingDispatchers.length < 1)
        {
            return;
        }

        send(MessageDispatcherMessages.AddMessageDispatchersMessage.builder()
                                                                   .sender(getSelf())
                                                                   .receiver(messageDispatcher)
                                                                   .messageDispatchers(existingDispatchers)
                                                                   .build());
    }
}
