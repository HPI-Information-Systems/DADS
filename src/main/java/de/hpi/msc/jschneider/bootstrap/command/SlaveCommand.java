package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;

@Getter
@Parameters(commandDescription = "starts a slave actor system")
public class SlaveCommand extends AbstractCommand
{
    @Parameter(names = "--master-host", description = "host of the MASTER system", required = true)
    private String masterHost;

    @Parameter(names = "--master-port", description = "port of the MASTER system")
    private int masterPort = DEFAULT_PORT;
}
