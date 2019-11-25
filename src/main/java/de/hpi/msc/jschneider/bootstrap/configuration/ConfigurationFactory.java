package de.hpi.msc.jschneider.bootstrap.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.val;
import lombok.var;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ConfigurationFactory
{
    private static final String DEFAULT_CONFIGURATION_NAME = "remote.conf";
    private static final String HOST_VARIABLE_PATTERN = "host";
    private static final String PORT_VARIABLE_PATTERN = "port";
    private static final String NUMBER_OF_THREADS_VARIABLE_PATTERN = "threads";

    public static Config createRemoteConfiguration(String host, int port, int numberOfThreads) throws FileNotFoundException
    {
        return loadConfiguration(DEFAULT_CONFIGURATION_NAME,
                                 new VariableBinding(HOST_VARIABLE_PATTERN, host),
                                 new VariableBinding(PORT_VARIABLE_PATTERN, port),
                                 new VariableBinding(NUMBER_OF_THREADS_VARIABLE_PATTERN, numberOfThreads));
    }

    private static Config loadConfiguration(String resourceName, VariableBinding... variables) throws FileNotFoundException
    {
        val inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (inputStream == null)
        {
            throw new FileNotFoundException(String.format("Unable to load configuration from \"%1$s\"", resourceName));
        }

        var content = new BufferedReader(new InputStreamReader(inputStream)).lines();
        for (val variable : variables)
        {
            content = content.map(variable::apply);
        }

        val result = content.collect(Collectors.joining("\n"));
        return ConfigFactory.parseString(result);
    }
}
