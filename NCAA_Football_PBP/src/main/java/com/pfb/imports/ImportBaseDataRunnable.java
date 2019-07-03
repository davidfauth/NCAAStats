package com.pfb.imports;


import com.pfb.schema.Labels;
import com.pfb.schema.RelationshipTypes;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


import java.util.HashMap;
import java.util.HashSet;

public class ImportBaseDataRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;
    

    public ImportBaseDataRunnable(String file, GraphDatabaseService db, Log log) {
        this.file = file;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        
        
        Reader in;
        Iterable<CSVRecord> records = null;
        try {
            in = new FileReader("/" + file);
            records = CSVFormat.EXCEL.withHeader().parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportBaseDataRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportBaseDataRunnable Import - IO Exception: " + file);
        }

        HashMap<Node, Node> hmpalletScans = new HashMap<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;
            int rowCounter = 0;
            Node previousPlay = null;
            Node previousGame = null;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                rowCounter ++;

                //if (count>1){
                    Node year = null;
                    if(!record.get(0).isEmpty()){
                        year = db.findNode(Labels.Year, "year", record.get(1));
                        if (year ==null){
                            year = db.createNode(Labels.Year);
                            year.setProperty("year",record.get(1));
                        }
                    }

                    Node game = null;
                    if(!record.get(0).isEmpty()){
                        game = db.findNode(Labels.Game, "gameId", record.get(0));
                        if (game ==null){
                            game = db.createNode(Labels.Game);
                            game.setProperty("gameId",record.get(0));
                            game.setProperty("year",record.get(1));
                            game.setProperty("week",record.get(2));
                        }
                        
                    }
                    Node homeTeam = null;
                    if(!record.get(0).isEmpty()){
                        homeTeam = db.findNode(Labels.Team, "teamName", record.get(4));
                        if (homeTeam ==null){
                            homeTeam = db.createNode(Labels.Team);
                            homeTeam.setProperty("teamName",record.get(4));
                            homeTeam.setProperty("teamAbbr",record.get(5));
                            homeTeam.setProperty("teamID",record.get(3));
                        }
                    }
                    Node awayTeam = null;
                    if(!record.get(0).isEmpty()){
                        awayTeam = db.findNode(Labels.Team, "teamName", record.get(7));
                        if (awayTeam ==null){
                            awayTeam = db.createNode(Labels.Team);
                            awayTeam.setProperty("teamName",record.get(7));
                            awayTeam.setProperty("teamAbbr",record.get(8));
                            awayTeam.setProperty("teamID",record.get(6));
                        }
                    }
                    ResourceIterator<Node> driveNode = null;
                    Node thisDriveNode = null;
                    if(!record.get(0).isEmpty()){
                        driveNode = db.findNodes(Labels.Drive, "driveIndex", record.get(9), "gameID", record.get(0));
                        if (!driveNode.hasNext()){
                            thisDriveNode = db.createNode(Labels.Drive);
                            thisDriveNode.setProperty("driveIndex",record.get(9));
                            thisDriveNode.setProperty("gameID",record.get(0));
                            thisDriveNode.setProperty("offenseTeam",record.get(12));
                            thisDriveNode.setProperty("defenseTeam",record.get(15));
                            
                            }else {
                            thisDriveNode = driveNode.next();
                        }
                    }
                    Node playNode = null;
                    if(!record.get(0).isEmpty()){
                        //playNode = db.findNode(Labels.Team, "teamName", record.get(7));
                        if (playNode ==null){
                            playNode = db.createNode(Labels.Play);
                            playNode.setProperty("playIndex",record.get(10));
                            playNode.setProperty("homeScore",record.get(17));
                            playNode.setProperty("awayScore",record.get(18));
                            playNode.setProperty("isScoringPlay",record.get(19));
                            playNode.setProperty("quarter",record.get(20));
                            playNode.setProperty("down",record.get(23));
                            playNode.setProperty("distance",record.get(24));
                            playNode.setProperty("yardLine",record.get(25));
                            playNode.setProperty("yardsGained",record.get(26));
                            playNode.setProperty("endYardLine",record.get(27));
                            playNode.setProperty("description",record.get(28));
                        }
                    }

                    if (playNode != null && thisDriveNode !=null)
                    {
                        thisDriveNode.createRelationshipTo(playNode, RelationshipTypes.HAS_PLAY_IN_DRIVE);
                    }

                    if (game != null && homeTeam !=null)
                    {
                        int found=0;
                        
                        for (Relationship r : game.getRelationships(RelationshipTypes.HAS_HOME_TEAM, Direction.OUTGOING)) {
                            String teamName = r.getEndNode().getProperty("teamName").toString();
                            if (teamName.equalsIgnoreCase(record.get(4))){
                                found=1;
                            }
                        }
                        if (found < 1){
                            game.createRelationshipTo(homeTeam, RelationshipTypes.HAS_HOME_TEAM);
                        }
                    }

                    if (game != null && awayTeam !=null)
                    {
                        int found=0;
                        
                        for (Relationship r : game.getRelationships(RelationshipTypes.HAS_AWAY_TEAM, Direction.OUTGOING)) {
                            String teamName = r.getEndNode().getProperty("teamName").toString();
                            if (teamName.equalsIgnoreCase(record.get(7))){
                                found=1;
                            }
                        }
                        if (found < 1){
                            game.createRelationshipTo(awayTeam, RelationshipTypes.HAS_AWAY_TEAM);
                        }
                    }

                    if (game != null && thisDriveNode !=null)
                    {
                        int found=0;
                        
                        for (Relationship r : game.getRelationships(RelationshipTypes.HAS_DRIVE, Direction.OUTGOING)) {
                            String driveNumString = r.getEndNode().getProperty("driveIndex").toString();
                            String gameID = r.getEndNode().getProperty("gameID").toString();
                            if (driveNumString.equalsIgnoreCase(record.get(9)) && gameID.equalsIgnoreCase(record.get(0))){
                                found=1;
                            }
                        }
                        if (found < 1){
                            game.createRelationshipTo(thisDriveNode, RelationshipTypes.HAS_DRIVE);
                        }
                    }
                    if (game != null && previousGame != null){
                        if (game.getId() == previousGame.getId()){
                            String relTypeString = record.get(22);
                            relTypeString=relTypeString.replaceAll(" ", "_");
                            relTypeString=relTypeString.replaceAll("\\(", "");
                            relTypeString=relTypeString.replaceAll("\\)", "");
                            RelationshipType relType = RelationshipType.withName(relTypeString);
                            try{    
                                previousPlay.createRelationshipTo(playNode, relType);
                            } catch (Exception e){
                                System.out.println(relTypeString);
                                System.out.println(count);
                            }
                        }
                    }
                    
                previousGame = game;
                previousPlay = playNode;

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            //}
            }
            tx.success();
        } finally {
            tx.close();
        }

    }

    

    
}
