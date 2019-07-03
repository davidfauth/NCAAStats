package com.pfb.imports;

import com.pfb.results.NodeResult;
import com.pfb.results.NodeListResult;
import com.pfb.results.StringResult;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;

import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.*;
import java.util.Map.Entry;

public class Imports {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    


    @Procedure(name = "com.pfb.import.importBaseData", mode = Mode.WRITE)
    @Description("CALL com.pfb.import.importBaseData(file)")
    public Stream<StringResult> importBaseData(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportBaseDataRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Base Data imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    
            
}
