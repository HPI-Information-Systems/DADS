package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;

@Getter
@Parameters(commandDescription = "starts a slave actor system")
public class SlaveCommand extends BaseCommand
{
    @Parameter(names = "--master-host", description = "host of the MASTER system", required = true)
    private String masterHost;
}