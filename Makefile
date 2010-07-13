JAVA		= /usr/java/jdk1.7.0/bin/java
BIN_DIR		= ./bin
JCIP_JAR	= /usr/lib/java/jcip-annotations.jar
SLF4J_JAR	= /usr/lib/java/slf4j-api-1.6.0.jar
SLF4J_IMPL	= /usr/lib/java/slf4j-jdk14-1.6.0.jar
CLASSPATH	= $(BIN_DIR):$(JCIP_JAR):$(SLF4J_JAR):$(SLF4J_IMPL)
JAVA_PROPS	= -Djava.util.logging.config.file=logging.properties
JAVA_FLAGS	= -cp $(CLASSPATH) $(JAVA_PROPS)

nexrad2-test:
	$(JAVA) $(JAVA_FLAGS) \
		edu.ucar.unidata.dynaccn.Publisher /tmp/publisher \
		| while read port; do \
	    echo '<?xml version="1.0" encoding="US-ASCII"?>' \
		>/tmp/subscription.xml; \
	    echo '<subscription>' >>/tmp/subscription.xml; \
	    echo '    <tracker host="localhost" port="'$$port'"/>' \
		>>/tmp/subscription.xml; \
	    echo '    <predicate type="everything"/>' >>/tmp/subscription.xml; \
	    echo '</subscription>' >>/tmp/subscription.xml; \
	    $(JAVA) $(JAVA_FLAGS) \
		edu.ucar.unidata.dynaccn.Subscriber \
		/tmp/subscriber /tmp/subscription.xml; \
	done
