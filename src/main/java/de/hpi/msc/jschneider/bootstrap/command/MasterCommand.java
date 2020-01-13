package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.msc.jschneider.bootstrap.command.validation.FileValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.StringToPathConverter;
import lombok.Getter;

import java.nio.file.Path;

@Getter
@Parameters(commandDescription = "starts a master actor system")
public class MasterCommand extends AbstractCommand
{
    @Parameter(names = "--min-slaves", description = "minimum number of slaves to start processing", required = true)
    private int minimumNumberOfSlaves;

    @Parameter(names = "--sequence", description = "record sequence to analyze", required = true, converter = StringToPathConverter.class, validateValueWith = FileValidator.class)
    private Path sequenceFilePath;

    @Parameter(names = "--sub-sequence-length", description = "length for the sub sequences", required = true)
    private int subSequenceLength;

    @Parameter(names = "--convolution-size", description = "size of the local convolution to transform subsequences into vectors", required = true)
    private int convolutionSize;

    @Parameter(names = "--sample-rate", description = "number of segments to create for the node creation", required = true)
    private int numberOfSamples;
}
