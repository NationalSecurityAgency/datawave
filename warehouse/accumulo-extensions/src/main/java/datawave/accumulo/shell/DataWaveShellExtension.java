package datawave.accumulo.shell;

import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellExtension;

/**
 * Registers the DataWave commands with the Accumulo shell. The jar containing this class has to be on the shell classpath, and the commands become available
 * after running {@code extensions -e}, at which point they are named {@code dw::<command>}.
 */
public class DataWaveShellExtension extends ShellExtension {

    public static final String EXTENSION_NAME = "dw";

    @Override
    public String getExtensionName() {
        return EXTENSION_NAME;
    }

    @Override
    public Command[] getCommands() {
        return new Command[] {new DataWaveScanCommand()};
    }
}
