# DATAWAVE Contributions

## Formatter

Please use the supplied code style formatter and code templates when developing for DATAWAVE. They can be added to 
Eclipse using the following steps:

* Formatter: Preferences > Java > Code Style > Formatter and import Eclipse-Datawave-Codestyle.xml
* Template: Preferences > Java > Code Style > Code Templates and import Eclipse-Datawave-Template.xml. make sure 
  to check the "Automatically add comments" box at the bottom of the window.

## ReadProperties Plugin

The `read-properties` directory contains a maven plugin to help with reading properties from multiple properties files,
with support for overrides/layered properties files.

## AssertProperties Plugin

The `assert-properties` directory contains a maven plugin to list required properties in a file, 
along with an error message to display for each property if it is not defined.

## JPoller Library

This third party library can be found [here](http://jpoller.sourceforge.net). Since this library does not publish jars to the central maven repository, we include them here for the time being.