#!/bin/bash
echo "Arguments:"
echo "<query>"
echo "<decorator>"
echo "Decorator can be EmptyDecorator, HtmlDecorator, or BashDecorator"

cd ../../../../../../../contrib/datawave-quickstart/bin/services/datawave/datawave-ingest-install/bin/ingest

findJars () {
    ls -1 ../../lib/$1-*.jar | sort | paste -sd ':' -
}

# Delete line 4 of ./ingest-libs.sh: ". ../ingest/ingest-env.sh"
sed -i '4d' ./ingest-libs.sh

. ./ingest-libs.sh

# Add ". ../ingest/ingest-env.sh" back on line 4 of ./ingest-libs.sh
sed -i '4 i . ../ingest/ingest-env.sh' ./ingest-libs.sh

java -cp $CLASSPATH:$(findJars jackson) datawave.query.jexl.visitors.JexlFormattedStringBuildingVisitor
