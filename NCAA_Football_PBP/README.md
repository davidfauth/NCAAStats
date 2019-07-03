# Import NCAA Football PBP
Import the Football PBP into Neo4j using a Stored Procedure

This project requires Neo4j 3.5.x

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/pfbimporter-0.1-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/pfbimporter-0.1-SNAPSHOT.jar neo4j-enterprise-3.5.6/plugins/.


Edit your Neo4j/conf/neo4j.conf file by adding this line:

    dbms.security.procedures.unrestricted=com.pfb.*
    
(Re)start Neo4j

Create the schema:

    CALL com.pfb.schema.generate;


Import the data: 

	CALL com.pfb.import.importBaseData("/Users/davidfauth/cfb_pbp/PBP_2017_Week_15.csv") 
    
    
