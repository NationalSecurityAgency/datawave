#!/bin/bash
echo "Arguments:"
echo "<query>"
echo "<decorator>"
echo "Decorator can be EmptyDecorator, HtmlDecorator, or BashDecorator"

cd ../../../../../../../contrib/datawave-quickstart/bin/services/datawave/datawave-ingest-install/bin/ingest

findJars () {
    ls -1 ../../lib/$1-*.jar | sort | paste -sd ':' -
}

. ./ingest-libs.sh

java -cp $CLASSPATH:$(findJars jackson) datawave.query.jexl.visitors.JexlFormattedStringBuildingVisitor
