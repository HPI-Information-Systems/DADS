package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.RootActorPath;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import java.util.regex.Pattern;

@NoArgsConstructor @EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ProcessorId
{
    private static final String ACTOR_SYSTEM_NAME_GROUP_NAME = "ActorSystemName";
    private static final String HOST_GROUP_NAME = "Host";
    private static final String PORT_GROUP_NAME = "Port";
    private static final Pattern EXTRACT_PATTERN = Pattern.compile(String.format("^akka:\\/\\/(?<%1$s>\\w+)-(?<%2$s>.+?)-(?<%3$s>\\d+)",
                                                                                 ACTOR_SYSTEM_NAME_GROUP_NAME,
                                                                                 HOST_GROUP_NAME,
                                                                                 PORT_GROUP_NAME));

    @EqualsAndHashCode.Include
    private String actorSystemName;
    @EqualsAndHashCode.Include
    private String host;
    @EqualsAndHashCode.Include
    private int port;

    public ProcessorId(String actorSystemName, String host, int port)
    {
        this.actorSystemName = actorSystemName;
        this.host = host.replaceAll("\\.", "_");
        this.port = port;
    }

    public static ProcessorId of(ActorSystem actorSystem)
    {
        return of(actorSystem.provider().getDefaultAddress().toString());
    }

    public static ProcessorId of(ActorRef actorRef)
    {
        return of(actorRef.path().root().toString());
    }

    public static ProcessorId of(RootActorPath rootActorPath)
    {
        return of(rootActorPath.toString());
    }

    @SneakyThrows
    public static ProcessorId of(String rootActorPath)
    {
        val matcher = EXTRACT_PATTERN.matcher(rootActorPath);
        if (!matcher.find())
        {
            throw new Exception(String.format("Unable to convert $1%s to ProcessorId!", rootActorPath));
        }

        return new ProcessorId(matcher.group(ACTOR_SYSTEM_NAME_GROUP_NAME),
                               matcher.group(HOST_GROUP_NAME),
                               Integer.parseInt(matcher.group(PORT_GROUP_NAME)));
    }

    @Override
    public String toString()
    {
        return String.format("%1$s-%2$s-%3$d",
                             actorSystemName,
                             host,
                             port);
    }
}
