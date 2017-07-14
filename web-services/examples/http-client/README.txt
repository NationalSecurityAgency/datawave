This project contains two different examples of how to reuse the Client.jar
distributed with the DATAWAVE web service to simplify parsing of web service
responses.

Example 1 uses JAX-B to deserialize XML responses from the web service back
into the POJOs included in Client.jar.  To run this example, execute
bin/jaxbQuery.sh.  You will need to pass a keystore and truststore
to the script.

Example 2 uses the Jackson library to deserialize JSON responses from the
web service back into the POJOs included in Client.jar.  To run this example,
execute bin/jacksonQuery.sh.  You will need to pass a keystore and
truststore to the script.