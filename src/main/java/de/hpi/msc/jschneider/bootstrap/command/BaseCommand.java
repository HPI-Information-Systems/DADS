package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import de.hpi.msc.jschneider.bootstrap.command.validation.DirectoryValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.StringToPathConverter;
import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
abstract class BaseCommand
{
    private static final int DEFAULT_NUMBER_OF_WORKERS = 4;
    private static final Path DEFAULT_WORKING_DIRECTORY = Paths.get("");

    @Parameter(names = "--host", description = "host address of THIS actor system", required = true)
    private String host;

    @Parameter(names = "--workers", description = "number of worker threads to utilize")
    private int numberOfWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @Parameter(names = "--working-dir", description = "working directory", converter = StringToPathConverter.class, validateValueWith = DirectoryValidator.class)
    private Path workingDirectory = DEFAULT_WORKING_DIRECTORY;
}
