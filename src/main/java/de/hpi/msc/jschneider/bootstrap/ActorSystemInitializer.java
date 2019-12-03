package de.hpi.msc.jschneider.bootstrap;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.AbstractCommand;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.bootstrap.command.SlaveCommand;
import de.hpi.msc.jschneider.bootstrap.configuration.ConfigurationFactory;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationProtocol;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRole;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

public class ActorSystemInitializer
{
    private static final Logger Log = LogManager.getLogger(ActorSystemInitializer.class);

    private static final String MASTER_ACTOR_SYSTEM_NAME = "MasterActorSystem";
    private static final String SLAVE_ACTOR_SYSTEM_NAME = "SlaveActorSystem";

    public static void runMaster(MasterCommand masterCommand) throws Exception
    {
        val actorSystem = initializeActorSystem(MASTER_ACTOR_SYSTEM_NAME, masterCommand);

        val processorRegistrationProtocol = ProcessorRegistrationProtocol.initialize(actorSystem, ProcessorRole.Worker, true);
        processorRegistrationProtocol.getRootActor().tell(ProcessorRegistrationMessages.RegisterAtMasterMessage.builder()
                                                                                                               .masterAddress(actorSystem.provider().getDefaultAddress())
                                                                                                               .build(), ActorRef.noSender());
        awaitTermination(actorSystem);
    }

    public static void runSlave(SlaveCommand slaveCommand) throws Exception
    {
        val actorSystem = initializeActorSystem(SLAVE_ACTOR_SYSTEM_NAME, slaveCommand);

        val processorRegistrationProtocol = ProcessorRegistrationProtocol.initialize(actorSystem, ProcessorRole.Worker, false);
        processorRegistrationProtocol.getRootActor().tell(ProcessorRegistrationMessages.RegisterAtMasterMessage.builder()
                                                                                                               .masterAddress(new Address(
                                                                                                                       "akka.tcp",
                                                                                                                       MASTER_ACTOR_SYSTEM_NAME,
                                                                                                                       slaveCommand.getMasterHost(),
                                                                                                                       slaveCommand.getMasterPort()))
                                                                                                               .build(), ActorRef.noSender());
        awaitTermination(actorSystem);
    }

    private static ActorSystem initializeActorSystem(String name, AbstractCommand command) throws Exception
    {
        val configuration = ConfigurationFactory.createRemoteConfiguration(command.getHost(), command.getPort(), command.getNumberOfThreads());

        SystemParameters.initialize(command, configuration);
        return ActorSystem.create(name, configuration);
    }

    private static void awaitTermination(ActorSystem actorSystem)
    {
        try
        {
            Await.ready(actorSystem.whenTerminated(), Duration.Inf());
        }
        catch (TimeoutException | InterruptedException exception)
        {
            exception.printStackTrace();
            System.exit(1);
        }

        Log.info("ActorSystem terminated gracefully.");
    }
}
