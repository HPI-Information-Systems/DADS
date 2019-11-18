package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.msc.jschneider.bootstrap.command.validation.FileValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.StringToPathConverter;
import lombok.Getter;

import java.nio.file.Path;

@Getter
@Parameters(commandDescription = "starts a master actor system")
public class MasterCommand extends BaseCommand
{
    @Parameter(names = "--sequence", description = "record sequence to analyze", required = true, converter = StringToPathConverter.class, validateValueWith = FileValidator.class)
    private Path sequenceFilePath;
}