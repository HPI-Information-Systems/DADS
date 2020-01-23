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
    private static final String HOST_GROUP_NAME = "Host";
    private static final String PORT_GROUP_NAME = "Port";
    private static final Pattern EXTRACT_PATTERN = Pattern.compile(String.format("^akka:\\/\\/(?<ActorSystemName>\\w+)-(?<%1$s>(?>\\d+_){3}(?>\\d+))-(?<%2$s>\\d+)",
                                                                                 HOST_GROUP_NAME,
                                                                                 PORT_GROUP_NAME));

    @EqualsAndHashCode.Include
    private String host;
    @EqualsAndHashCode.Include
    private int port;

    public ProcessorId(String host, int port)
    {
        this.host = host.replaceAll("\\.", "_");
        this.port = port;
    }

    public static ProcessorId of(ActorSystem actorSystem)
    {
        return of(actorSystem.provider().getDefaultAddress().toString());
    }

    public static ProcessorId of(ActorRef actorRef)
    {
        return of(actorRef.path().root());
    }

    public static ProcessorId of(RootActorPath rootActorPath)
    {
        return of(rootActorPath.toString());
    }

    @SneakyThrows
    public static ProcessorId of(String rootActorPath)
    {
        val matcher = EXTRACT_PATTERN.matcher(rootActorPath);
        if (!matcher.matches())
        {
            throw new Exception(String.format("Unable to convert $1%s to ProcessorId!", rootActorPath));
        }

        return new ProcessorId(matcher.group(HOST_GROUP_NAME), Integer.parseInt(matcher.group(PORT_GROUP_NAME)));
    }

    @Override
    public String toString()
    {
        return host + "-" + port;
    }
}
