package com.pfb.schema;

import com.pfb.results.StringResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.stream.Stream;

public class Schema {

    @Context
    public GraphDatabaseService db;


    @Procedure(name="com.pfb.schema.generate",mode= Mode.SCHEMA)
    @Description("CALL com.pfb.schema.generate() - generate schema")

    public Stream<StringResult> generate() throws IOException {
        org.neo4j.graphdb.schema.Schema schema = db.schema();
        if (!schema.getConstraints(Labels.Game).iterator().hasNext()) {
            schema.constraintFor(Labels.Game)
                    .assertPropertyIsUnique("gameId")
                    .create();
        }

        if (!schema.getConstraints(Labels.Team).iterator().hasNext()) {
            schema.constraintFor(Labels.Team)
                    .assertPropertyIsUnique("teamName")
                    .create();
        }

        if (!schema.getConstraints(Labels.Year).iterator().hasNext()) {
                schema.constraintFor(Labels.Year)
                        .assertPropertyIsUnique("year")
                        .create();
            }

        try {
            schema.indexFor(Labels.Drive)
            .on("driveIndex")
            .on("gameID")
            .create();
        } catch (Exception e) {
            System.out.println(e);
        }

        return Stream.of(new StringResult("Schema Generated"));
    }

}
