JAVA		= /usr/java/jdk1.7.0/bin/java
BIN_DIR		= ../output
SLF4J_JAR	= /usr/lib/java/slf4j-api-1.6.0.jar
SLF4J_IMPL	= /usr/lib/java/slf4j-jdk14-1.6.0.jar
CLASSPATH	= $(BIN_DIR):$(SLF4J_JAR):$(SLF4J_IMPL)
JAVA_PROPS	= -Djava.util.logging.config.file=../logging.properties
JAVA_FLAGS	= -cp $(CLASSPATH) $(JAVA_PROPS)
SUBSCRIPTION	= /tmp/subscription.xml
NEXRAD2_KFTG	= /web/content/staff/steve/dynaccn/nexrad2-kftg.udda

nexrad2-test:
	rm -rf /tmp/subscriber /tmp/publisher
	-mkdir /tmp/publisher
	$(JAVA) $(JAVA_FLAGS) \
		edu.ucar.unidata.dynaccn.Publisher /tmp/publisher \
		| while read port; do \
	    echo '<?xml version="1.0" encoding="US-ASCII"?>' \
		>$(SUBSCRIPTION); \
	    echo '<subscription>' >>$(SUBSCRIPTION); \
	    echo '    <tracker host="gilda.unidata.ucar.edu" ' \
	    	'port="'$$port'"/>' >>$(SUBSCRIPTION); \
	    echo '    <predicate type="everything"/>' >>$(SUBSCRIPTION); \
	    echo '</subscription>' >>$(SUBSCRIPTION); \
	    cp $(SUBSCRIPTION) $(NEXRAD2_KFTG); \
	    $(JAVA) $(JAVA_FLAGS) \
		edu.ucar.unidata.dynaccn.Subscriber \
		/tmp/subscriber $(SUBSCRIPTION); \
	done
