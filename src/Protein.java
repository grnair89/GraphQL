import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
/**
 *
 * Protein class is used for subgraph matching using refine search space algorithm for proteins dataset.
 * @author Shrinivas Arun Joshi
 * @author Ganesh Rajasekharan
 */

/**
 * The following class Protein uses refine search method to reduce the search space size and then runs
 * search method to get all matched subgraphs from the target graph.
 */
public class Protein
{   static String processingOrder="";
    static ArrayList<Integer> solution=new ArrayList<>();
    static Map<Integer,Node> sol=new HashMap<>();
    static ArrayList<Map<Integer,Node>> completeSolution=new ArrayList<>();

    static ArrayList<Integer> sol_ID=new ArrayList<>();
    //static String ComputationalOrder="";
    static Queue<Integer> ComputationalOrder = new LinkedList<Integer>();
    static int cnt=0;

    /**
     * This method is used for loading protein database
     * @param File - protein file to be loaded
     * @param myInsert - BatchInserter
     */

    public static void targetData(String File,BatchInserter myInsert)
    {
        // br=null;
        try {
            Scanner sc=new Scanner(new FileReader(File));
            Map<Integer,Long> proteinNodes=new HashMap<>();
            String path=new String(File);
            String[] pathContent=path.split("\\\\");
            String Folder="C:\\Protein\\"+pathContent[pathContent.length-1];

            myInsert=BatchInserters.inserter(new File(Folder));
            boolean flag=false;
            while(sc.hasNext())
            {
                int n=Integer.parseInt(sc.nextLine());

                if(!flag) {
                    while (n > 0)
                    {

                        String line = sc.nextLine();
                        {
                            String[] content = line.split("\\s");
                            //System.out.println(line);
                            int ID = Integer.parseInt(content[0]);
                            String label = content[1];
                            //System.out.println(ID + " : " + content[1]);
                            Map<String, Object> protein = new HashMap<>();
                            protein.put("ID", ID);

                            long nodeID = myInsert.createNode(protein, Label.label(label));
                            proteinNodes.put(ID, nodeID);

                        }

                        n--;
                    }
                    flag = true;
                }
                else
                {

                    while (n > 0)
                    {
                        String line = sc.nextLine();
                        String[] content=line.split("\\s");
                        //System.out.println(line);

                        int ID1=Integer.parseInt(content[0]);
                        int ID2=Integer.parseInt(content[1]);
                        Long nodeID1=proteinNodes.get(ID1);
                        Long nodeID2=proteinNodes.get(ID2);
                        myInsert.createRelationship(nodeID1, nodeID2, RelationshipType.withName(""), null);
                        n--;
                    }


                }
            }

            myInsert.shutdown();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch(IOException e)
        {
            e.printStackTrace();
        }
    }
    /**
     * This method is used to load the target Igraph database
     * @param File Igraph file to be loaded
     * @param myInsert - BatchInserter
     */
    public static void targetIGraph(String File,BatchInserter myInsert)
    {
        try
        {

            String path=new String(File);

            String Folder="C:\\iGraph\\";
            if(File.contains("human"))
                Folder+="human";
            else
                Folder+="yeast";
            myInsert=BatchInserters.inserter(new File(Folder));
            BufferedReader br=new BufferedReader(new FileReader(File));
            String str="";
            Map<Integer,Long> iGraph=new HashMap<>();
            while((str=br.readLine())!=null)
            {
                if(str.contains("t"))
                {
                    //System.out.println(str);
                }
                else if(str.contains("v"))
                {

                    String[] content = str.split("\\s");

                    int ID = Integer.parseInt(content[1]);
                    String label = content[2];
                    //System.out.println(ID + " : " + content[2]);
                    Map<String, Object> protein = new HashMap<>();
                    protein.put("ID", ID);


                    Label[] labelArray=new Label[content.length-2];

                    // System.out.println("In"+content.length);
                    for(int i=2;i<content.length;i++)
                    {

                        labelArray[i-2]=Label.label(content[i]);
                        //System.out.println("Adding label "+content[i]);
                    }

                    long nodeID = myInsert.createNode(protein, labelArray);
                    iGraph.put(ID,nodeID);

                }
                else if(str.contains("e"))
                {

                    String[] content = str.split("\\s");
                    int ID1=Integer.parseInt(content[1]);
                    int ID2=Integer.parseInt(content[2]);
                    Long nodeID1=iGraph.get(ID1);
                    Long nodeID2=iGraph.get(ID2);
                    myInsert.createRelationship(nodeID1, nodeID2, RelationshipType.withName(content[3]), null);
                    myInsert.createRelationship(nodeID2, nodeID1, RelationshipType.withName(content[3]), null);
                    //System.out.println("Create relationship");

                }


            }

            myInsert.shutdown();
            //db.shutdown();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }


    public static void IGraph(String File,BatchInserter myInsert)
    {
        try
        {
            GraphDatabaseService db = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder(new File("C:\\iGraph\\"))
                    .setConfig(GraphDatabaseSettings.pagecache_memory, "512M" )
                    .setConfig(GraphDatabaseSettings.string_block_size, "60" )
                    .setConfig(GraphDatabaseSettings.array_block_size, "300" )
                    .newGraphDatabase();

            String path=new String(File);

            String Folder="C:\\iGraph\\";
            //myInsert=BatchInserters.inserter(new File(Folder));
            BufferedReader br=new BufferedReader(new FileReader(File));
            String str="";
            Map<Integer,Long> iGraph=new HashMap<>();
            while((str=br.readLine())!=null)
            {
                if(str.contains("t"))
                {
                    System.out.println(str);
                }
                else if(str.contains("v"))
                {
                    //myInsert.createNode()
                    String[] content = str.split("\\s");

                    int ID = Integer.parseInt(content[1]);
                    String label = content[2];
                    System.out.println(ID + " : " + content[2]);
//                    Map<String, Object> protein = new HashMap<>();
//                    protein.put("ID", ID);

                    try(Transaction trax=db.beginTx())
                    {
                        Node nodeVertex = db.createNode(Label.label(content[2]));
                        nodeVertex.setProperty("ID", ID);

                        for(int i=3;i<content.length;i++)
                        {
                            nodeVertex.addLabel(Label.label(content[i]));
                        }
                        trax.success();
                    }
                    System.out.println("In"+content.length);


                }
                else if(str.contains("e"))
                {

                    try(Transaction trax=db.beginTx()) {
                        String[] content = str.split("\\s");
                        String NODE1 = content[1];
                        String NODE2 = content[2];
                        String Label = content[3];

                        Node node1 = db.getNodeById(Long.parseLong(NODE1));
                        Node node2 = db.getNodeById(Long.parseLong(NODE2));

                        node1.createRelationshipTo(node2, RelationshipType.withName(Label));

                        System.out.println("Created Realtionship");

                        trax.success();
                    }
                }


            }

            // myInsert.shutdown();
            db.shutdown();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }


    public static Map<Integer,Query> QueryProtein(String File,BatchInserter myInsert)
    {
        Map<Integer,Query> query=new HashMap<>();
        try
        {
            Scanner sc=new Scanner(new FileReader(File));
            Map<Integer,Long> proteinNodes=new HashMap<>();
            String path=new String(File);
            String[] pathContent=path.split("\\\\");

            //String Folder="C:\\Protein\\"+pathContent[pathContent.length-1];

            String vertexLine="";
            String edgeLine="";
            Set<String> vertexLines=new HashSet<>();
            Set<String> edgeLines=new HashSet<>();



            boolean flag=false;
            while(sc.hasNext())
            {
                String str=sc.nextLine();
                int n=Integer.parseInt(str);
                //System.out.println(n);
                //vertexLines.add(str);
                if(!flag) {
                    vertexLine+=str+"\n";
                    while (n > 0)
                    {

                        String line = sc.nextLine();
                        {
                            vertexLines.add(line);
                            vertexLine+=line+"\n";
                            String[] content = line.split("\\s");

                            Query obj=new Query();
                            obj.setID(Integer.parseInt(content[0]));
                            obj.setLabel(content[1]);
//                            Set<Integer> queryEdge=obj.getEdges();
//                            queryEdge.add();
                            query.put(Integer.parseInt(content[0]),obj);
                        }

                        n--;
                    }
                    flag = true;
                }
                else
                {
                    edgeLine+=str+"\n";
                    while (n > 0)
                    {
                        String line = sc.nextLine();
                        edgeLines.add(line);
                        edgeLine+=line+"\n";
                        String[] content = line.split("\\s");

                        if(query.containsKey(Integer.parseInt(content[0])))
                        {
                            Query obj=query.get(Integer.parseInt(content[0]));
                            Set<Integer> queryEdge=obj.getEdges();
                            queryEdge.add(Integer.parseInt(content[1]));

                        }

                        n--;
                    }

                }
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return query;
    }



    public static Map<Integer,ArrayList<Node>> getSearchSpace(Map<Integer,Query> query,GraphDatabaseService db)
    {
        Node[] phi=null;
        ArrayList<Node> nodes=new ArrayList<>();
        //ArrayList<Node> phiList=new ArrayList<>();
        Map<Integer,ArrayList<Node>> SearchSpace=new HashMap<>();
        try {

            Set<String> Labels = new HashSet<>();

            for (Integer vertex : query.keySet())
            {
                Query object = query.get(vertex);
                //Take one object at a time from the label and start adding
                ArrayList<Node> mappedNodes = new ArrayList<>();

                try (Transaction trax = db.beginTx()) {
                    try (ResourceIterator<Node> allNodes = db.findNodes(Label.label(object.getLabel()))) {

                        while (allNodes.hasNext())
                        {
                            Node n = allNodes.next();
                            //System.out.println("Inside find all nodes");
                            nodes.add(n);
                            mappedNodes.add(n);
                        }

                    }
                    trax.success();

                }
                Labels.add(object.getLabel());
                SearchSpace.put(vertex, mappedNodes);

            }


//            for (Integer vertex : SearchSpace.keySet())
//            {
//                try (Transaction trax = db.beginTx())
//                {
//                    ArrayList<Node> nodeList=SearchSpace.get(vertex);
//                    System.out.println("Printing for vertex "+vertex);
//                    System.out.println("Size of Nodelist for vertex "+vertex +" "+nodeList.size());
//                    trax.success();
//                }
//
//            }

            //System.out.println("Done");

        }catch(Exception e)
        {
            e.printStackTrace();
        }


        return SearchSpace;
    }
    static int counter = 0;
    public static void Search(int i,int[] SearchOrder,Map<Integer,Query> query,GraphDatabaseService db,Map<Integer, ArrayList<Node>> phi)
    {

        try {

            //
            ArrayList<Node> phi_Ui=phi.get(SearchOrder[i]);

            //System.out.println(sol.size());

            if(counter<1000){
                for(Node n:phi_Ui)
                {

                    if(sol.size()>i)
                    {
                        //System.out.println(sol.toString());
                        // counter ++;

                        sol.remove(SearchOrder[i]);

                    }


                    boolean flag=false;
                    for(Integer s:sol.keySet())
                    {
                        if(sol.get(s).equals(n))
                        {
                            flag=true;
                            break;
                        }
                    }
                    if(flag)
                        continue;

                    if(!Check(SearchOrder[i],n,query,db,sol,i,SearchOrder))
                        continue;

                    sol.put(SearchOrder[i],n);

                    if(i<SearchOrder.length - 1)
                    {
                        Search(i+1 , SearchOrder, query, db, phi);
                    }
                    else
                    {
                        if(query.size()==sol.size())
                        {

                            if(counter<10000)
                            {
                                //completeSolution.add(sol);
                                System.out.println(sol);
                                counter++;
                            }
                            else{

                                //sol.remove(i);
                                sol.remove(SearchOrder[i]);
                                return;
                            }

                            //sol.remove(i);
                            sol.remove(SearchOrder[i]);
                        }
                    }


                }

            }


        }catch (Exception e)
        {
            e.printStackTrace();
        }

    }


    public static boolean Check(int ID,Node v,Map<Integer,Query> query,GraphDatabaseService db,Map<Integer,Node> sol,int i,int[] SearchOrder)
    {
// get id of element for which you are searching
// get the node currently under consideration
//
// for all the nodes in partial solution
// check if current node is in there relations (or edges)
// if it's present go ahead and check for next element in partial solution
// else set temporary flag as false

// check temp flag and if its false then retunr false

// if you reach end of function return true.
        Query obj = query.get(ID);
        Set<Integer> edges = obj.getEdges();//{3:1,2}
        //System.out.println("Inside check");

        for(int j=0;j<i;j++)
        {
            boolean flag=false;
            if(edges.contains(SearchOrder[j]))
            {
                try (Transaction trax = db.beginTx())
                {

                    for (Relationship relation : v.getRelationships(Direction.BOTH))
                    {
                        //flag=false;
                        //if (relation.getOtherNode(v).equals(sol.get(j)))
                        if (relation.getOtherNode(v).equals(sol.get(SearchOrder[j])))
                        {
                            flag= true;
                            break;
                        }

                    }
                    trax.success();
                    if(flag==false)
                    {
                        //              System.out.println("Returning false");
                        return false;
                    }
                }


            }
            //j++;
        }

        //System.out.println("Returning true");
        return true;

    }

    public static Query[] getSearchOrder(Map<Integer,Query> query)
    {
        Query[] SearchOrder=new Query[query.size()];
        int i=0;

        for(Integer vertex:query.keySet())
        {
            //System.out.println("In");
            SearchOrder[i]=query.get(vertex);
            i++;
        }
        return SearchOrder;
    }

    public static void FunDFS(int vertexNumber,Map<Integer,Query> query,boolean[] visited,String str)
    {
        visited[vertexNumber]=true;
        //System.out.println(vertexNumber+" --> ");
        str+=vertexNumber+" --> ";
        processingOrder+=vertexNumber+"-";
        Query obj=query.get(vertexNumber);
        Set<Integer> edges=obj.getEdges();
        for(Integer i:edges)
        {
            if(!visited[i])
                FunDFS(i,query,visited,str);
        }

    }

    public static String DFS(Map<Integer,Query> query)
    {
        boolean[] visited=new boolean[query.size()];

        String str="";
        for(int j=0;j<visited.length;j++)
        {
            if(visited[j]==false)
            {
                FunDFS(j,query,visited,str);
            }

        }

        return str;
    }
    /**
     * This method is used to create a data structure for igraph queries.
     * @param File input query file
     * @return a map of queries where each query contains query vertex number and a query object which contains its neighbors
     */
    public static Map<Integer,Map<Integer,Query>> iGraph_query(String File)
    {
        Map<Integer,Query> subGraph=new HashMap<>();//vertex ID, vertex object
        Map<Integer,Map<Integer,Query>> queryMap=new HashMap<>();//<query number,Map of query<ID,Query>>
        try
        {

            List<String> querySFile = java.nio.file.Files.readAllLines(new File(File).toPath(), StandardCharsets.UTF_8);

            ArrayList<String> QUE =new ArrayList();

            int size=querySFile.size();

            int queryNumber=0;
            for(int i=0;i<size;i++)
            {
                Map<Integer,Query> tmp=null;
                if((querySFile.get(i).contains("t")))
                {
                    String str=querySFile.get(i);
                    String[] content=str.split("\\s");
                    queryNumber=Integer.parseInt(content[2]);
                    tmp=new HashMap<>();
                    queryMap.put(queryNumber,tmp);
                }

                else if (querySFile.get(i).contains("v"))
                {
                    Map<Integer,Query> temp=queryMap.get(queryNumber);
                    String str=querySFile.get(i);
                    String[] content=str.split("\\s");

                    //if(!temp.containsKey(Integer.parseInt(content[1])))
                    Query query = new Query();
                    query.setLabel(content[2]);
                    query.setID(Integer.parseInt(content[1]));
                    temp.put(Integer.parseInt(content[1]), query);


                }
                else if(querySFile.get(i).contains("e"))
                {
                    Map<Integer,Query> temp=queryMap.get(queryNumber);
                    String str=querySFile.get(i);
                    String[] content=str.split("\\s");

                    Query query=temp.get(Integer.parseInt(content[1]));
                    Set<Integer> edges=query.getEdges();
                    edges.add(Integer.parseInt(content[2]));

                    if(temp.containsKey(Integer.parseInt(content[2])))
                    {

                        Query queryTmp=temp.get(Integer.parseInt(content[2]));
                        Set<Integer> edgesTmp=queryTmp.getEdges();
                        edgesTmp.add(Integer.parseInt(content[1]));

                    }


                }



            }

            // System.out.println(QUE.size());


//
//            System.out.println("Print");
//            for(Integer k:queryMap.keySet())
//            {
//                Map<Integer,Query> obj=queryMap.get(k);
//
//                for(Integer v:obj.keySet())
//                    System.out.println(obj.get(v).toString());
//                System.out.println("\n");
//            }


        } catch (Exception e)
        {
            e.printStackTrace();
        }

        return queryMap;
    }
    /**
     * This method is used to create a data structure for protein  queries.
     * @param File input query file
     * @return a map of the query which contains query vertex number and its corresponding query object which contains its neighbors
     */
    public static Map<Integer,Query> ProteinQuery(String File)
    {
        Map<Integer,Query> subGraph=new HashMap<>();//vertex ID, vertex object
        try
        {
            List<String> queryFile = java.nio.file.Files.readAllLines(new File(File).toPath(), StandardCharsets.UTF_8);

            boolean flag=false;
            for(int i=0;i<queryFile.size();i++)
            {
                int length=Integer.parseInt(queryFile.get(i));

                System.out.println(length);
                while(length >0 )
                {

                    i++;
                    String str=queryFile.get(i);
                    String[] content=str.split("\\s");

                    if(queryFile.get(i).matches(".*[a-zA-Z]+.*"))
                    {

                        Query obj=new Query();
                        obj.setID(Integer.parseInt(content[0]));
                        obj.setLabel(content[1]);
                        subGraph.put(Integer.parseInt(content[0]),obj);

                    }
                    else
                    {

                        Query obj=subGraph.get(Integer.parseInt(content[0]));
                        Set<Integer> edges=obj.getEdges();
                        edges.add(Integer.parseInt(content[1]));

                    }

                    length--;
                }


            }


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return subGraph;

    }
    /**
     * This method returns reduced searchspace by using neighborhood profiling in the form of a map for the given
     * input query and database
     * @param query input query map
     * @param db database
     * @return searchspace
     */
    public static Map<Integer,ArrayList<Node>> neighbourProfile(Map<Integer,Query> query,GraphDatabaseService db)
    {
        //System.out.println("InNeighbor");

        Map<Integer,ArrayList<Node>> SearchSpace=new HashMap<>();
        try
        {
            for (Integer vertex : query.keySet())
            {
                Query object = query.get(vertex);
                ArrayList<Node> mappedNodes = new ArrayList<>();

                //String neighborProfile=originalProfile.replaceAll("[^a-zA-Z0-9]", "");
                String originalProfile="";
                ArrayList<String> oP=new ArrayList<>();

                for(Integer adjacentVertex:object.getEdges())
                {
                    Query obj=query.get(adjacentVertex);
                    originalProfile+=obj.getLabel();

                }
                // System.out.println(originalProfile);

                //originalProfile+=object.getLabel();
                //Arrange lexicographically
                char[] Sorted=originalProfile.toCharArray();
                Arrays.sort(Sorted);

                for(char ck:Sorted)
                    oP.add(ck+"");

                //System.out.println("Looking for Vertex with label "+object.getLabel()+" having neighbor profile "+neighborProfile);

                try (Transaction trax = db.beginTx())
                {
                    try (ResourceIterator<Node> allNodes = db.findNodes(Label.label(object.getLabel())))
                    {

                        int count=0;
                        while (allNodes.hasNext())
                        {
                            int l=1;
                            Node node = allNodes.next();

//                            count++;
//                            System.out.println(count);
                            String profile="";

                            for(Relationship relation:node.getRelationships(Direction.BOTH))
                            {

                                Node otherNode=relation.getOtherNode(node);
                                //
                                //ArrayList<Label> lab=otherNode.getLabels().iterator().next();

                                Iterator<Label> iterator=otherNode.getLabels().iterator();
                                while(iterator.hasNext())
                                {
                                    //lab.add(otherNode.getLabels().iterator().next().name());
                                    Label lab=iterator.next();
                                    profile+=lab.name();
                                    //iterator=iterator.next();
                                }
                                //List<>
                                //profile+=otherNode.getLabels().iterator().next().name();

                                //otherNode.ge
                                //orderedMap.put(otherNode.getLabels().iterator().next().name(),otherNode);
                            }
                            //node.addLabel(Label.label(profile));
                            //System.out.println(""+profile);

                            char[] profileSorted=profile.toCharArray();
                            Arrays.sort(profileSorted);
                            //String actualNeighborProfile=String.valueOf(profileSorted);
                            ArrayList<String> nP=new ArrayList<>();
                            for(char c:profileSorted)
                                nP.add(c+"");


                            for(String st:oP) {
                                if (nP.contains(st)) {
                                    nP.remove(st);
                                }else{
                                    l=0;
                                }
                            }
                            if(l==1) {
                                mappedNodes.add(node);
                            }


                        }



                    }
                    trax.success();

                }

                SearchSpace.put(vertex,mappedNodes);
            }



        }
        catch(Exception e)
        {
            e.printStackTrace();
        }


        return SearchSpace;
    }


    public static int findMinimum(Set<Integer> Edges,Map<Integer,Query> query,boolean flag,Set<Integer> processed,Map<Integer, ArrayList<Node>> SearchSpace)
    {
        float valueOfGamma=0.5f;
        int minPosition=0;
        int minimum=Integer.MAX_VALUE;

        Set<Integer> allEdges = new HashSet<>();

        for (int i: ComputationalOrder) {
            for (int j: query.get(i).getEdges()) {
                allEdges.add(j);
            }
        }

        for(int j:allEdges)
        {
            if(!flag)
            {
                valueOfGamma=(float) Math.pow(valueOfGamma, 0);
            }
            else
            {
                int countRelations=0;
                for(int k:query.get(j).getEdges())
                {
                    //if(processingOrder.contains(String.valueOf(k)))
                    if(ComputationalOrder.contains(k))
                    {
                        countRelations++;
                    }
                }
                valueOfGamma = (float) Math.pow(valueOfGamma, countRelations);

                float mul=SearchSpace.get(j).size()*valueOfGamma;

                if(!processed.contains(j) &&  mul< minimum )
                {
                    minPosition=j;
                    minimum=(int)mul;

                }

            }
        }

        return minPosition;
    }

    public static void getBestSearchOrder(Map<Integer,Query> query,int index,Set<Integer> processed,Map<Integer, ArrayList<Node>> SearchSpace)
    {
        if(!processed.contains(index))
        {
            processed.add(index);
            processingOrder+=index;
            ComputationalOrder.add(index);
            Set<Integer> edges=query.get(index).getEdges();
            int minimum=0;
            if(cnt==0) {
                minimum = findMinimum(edges, query, false, processed, SearchSpace);
                cnt++;
            }
            else
                minimum = findMinimum(edges, query, true, processed, SearchSpace);
            getBestSearchOrder(query,minimum,processed,SearchSpace);

            for(Integer vertex:query.keySet())
            {

                if(!ComputationalOrder.contains(vertex))
                    getBestSearchOrder(query,vertex,processed,SearchSpace);

            }

        }

    }

    /**
     * The following method count maximum bipartite matching using Kuhns' Algorithm
     * @param bipartiteGraph - bipartite graph
     * @return count of maximum matches
     */
    static int maxBipartiteMatches(boolean bipartiteGraph[][])

    {
        int rowSize = bipartiteGraph.length;
        int colSize = bipartiteGraph[0].length;

        int matches = 0;

        HashMap<Integer, Integer> matchMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < colSize; i++) {
            matchMap.put(i, Integer.MIN_VALUE);
        }

        for (int leftVertex = 0; leftVertex < rowSize; leftVertex++) {
            HashMap<Integer, Boolean> visitedMap = new HashMap<Integer, Boolean>();
            for (int i = 0; i < colSize; i++) {
                visitedMap.put(i, false);
            }
            if (isBipartiteMatch(bipartiteGraph, visitedMap, matchMap, leftVertex))
                matches += 1;
        }
        return matches;

    }

    /**
     * The following method checks whether there exists Bipartite matching
     * @param bipartiteGraph - the bipartite graph
     * @param visitedMap - visitedMap of visited nodes of boolean array
     * @param matchMap - Map used for marking
     * @param leftVertex - left vertex of disjoint set
     * @return
     */

    static boolean isBipartiteMatch(boolean bipartiteGraph[][], HashMap<Integer, Boolean> visitedMap, HashMap<Integer, Integer> matchMap, int leftVertex) {
        int colSize = bipartiteGraph[0].length;
        for (int rightVertex = 0; rightVertex < colSize; rightVertex++) {
            if (bipartiteGraph[leftVertex][rightVertex] && visitedMap.get(rightVertex) == false) {
                visitedMap.put(rightVertex, true);

                if (matchMap.get(rightVertex) == Integer.MIN_VALUE) {

                    matchMap.put(rightVertex, leftVertex);
                    return true;
                }

                if (isBipartiteMatch(bipartiteGraph, visitedMap, matchMap, matchMap.get(rightVertex))) {
                    matchMap.put(rightVertex, leftVertex);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The Method refineSearchSpace implements the refine Search space algorithm
     * and reduces the search space size effectively.
     *
     * @param level - refinement level
     * @param query - Pattern graph
     * @param phi1 - Search space that needs to be reduced
     * @param db1  - database
     */
    public static void refineSearchSpace(int level, Map<Integer, Query> query, Map<Integer, ArrayList<Node>> phi1, GraphDatabaseService db1){
        //       val mark = HashMap [String, Boolean] ()
        Map<String,Boolean> markMap=new HashMap();

        for(Integer u:query.keySet())
        {
            ArrayList<Node> obj=phi1.get(u);
            for(Node v:obj)
            {
                markMap.put(u+" "+v,true);
            }

        }


        for(int i=1;i<=level;i++)
        {

            for(Integer u:query.keySet())
            {
                ArrayList<Node> obj=phi1.get(u);
                Iterator it = obj.iterator();
                while(it.hasNext())
                {	Node v= (Node) it.next();
                    if(markMap.get(u+" "+v))
                    {
                        boolean[][] bipartiteGraph=new boolean[query.size()][4674];

                        //get u's neighbours
                        Query vertex=query.get(u);
                        Set<Integer> neighboursU=vertex.getEdges();

                        //get v's neighbours
                        Set<Integer> neighboursV=new HashSet<>();
                        Set<Node> neighbourNodev=new HashSet<>();
                        try (Transaction trax = db1.beginTx())
                        {

                            for (Relationship relation : v.getRelationships(Direction.BOTH)) {

                                Node otherNode = relation.getOtherNode(v);
                                neighboursV.add((int)otherNode.getId());
                                neighbourNodev.add(otherNode);

                                trax.success();


                            }


                        }
                        //Construct the bipartite graph
                        for(Integer Uneighbor:neighboursU)
                        {
                            // for(Integer Vneighbor:neighboursV)
                            for(Node Vneighbor:neighbourNodev)
                            {

                                if(phi1.get(Uneighbor).contains(Vneighbor))
                                {

                                    bipartiteGraph[Uneighbor][(int)Vneighbor.getId()]=true;

                                }

                            }

                        }


                        //Using KUHN's Algorithm for Bipartite matching
                        int maxBipartiteSize=maxBipartiteMatches(bipartiteGraph);


                        if(maxBipartiteSize==neighboursU.size())
                        {
                            //System.out.println("Setting : "+u+" "+v+" ,false");
                            markMap.put(u+" "+v,false);
                        }
                        else
                        {
                            //obj = phi1.get(u);
                            it.remove();
                            //
                            for(Integer Uneighbor:neighboursU)
                            {
                                // for(Integer Vneighbor:neighboursV)
                                for(Node Vneighbor:neighbourNodev)
                                {

                                    if(phi1.get(Uneighbor).contains(Vneighbor))
                                    {

                                        markMap.put(Uneighbor+" "+Vneighbor,true);

                                        //System.out.println("Setting : "+u+" "+v+" ,true");
                                        // bipartiteGraph[Uneighbor][(int)Vneighbor.getId()]=true;

                                    }

                                }

                            }



                        }

                    }
                }

                phi1.put(u, obj);

                //System.out.println("Done with vertex number : "+u);

            }


            // no pair (u, v) in mark => no further pruning
            List<Boolean> list=new ArrayList<Boolean>(markMap.values());
            if(!list.contains(true))
                break;



        }


    }



    public static void main(String[] args)
    {
        BatchInserter myInsert=null;
        try {

            File folder=new File("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\Proteins\\Proteins\\part3_Proteins\\Proteins\\target\\");
            File[] files=folder.listFiles();

            //loading all protein target set
            for(File file:files)
            {
                String filename=file.toString();

                //targetData(filename, myInsert); //Comment this line and uncommnet the targetData line just below this function and run the code to
                //load only one database.
            }

            //Loading protein files
            targetData("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\Proteins\\Proteins\\part3_Proteins\\Proteins\\target\\backbones_1CJF.grf", myInsert);

            //yeast database

            targetIGraph("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\iGraph\\iGraph\\yeast.igraph",myInsert);

            //Store igraph queries

            Map<Integer,Map<Integer,Query>> iGraphQ_human=iGraph_query ("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\iGraph\\iGraph\\human_q10.igraph");
            Map<Integer, Map<Integer,Query>> iGraphQ_yeast = iGraph_query("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\iGraph\\iGraph\\yeast_q10.igraph");


            //you can store queries for protein,just pass the entire path of the input file

            //Map<Integer,Query> query=ProteinQuery("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\Proteins\\Proteins\\part3_Proteins\\Proteins\\query\\mus_musculus_2LCK.16.sub.grf");
            Map<Integer,Query> query=ProteinQuery("C:\\Users\\Shrinivas\\Desktop\\GraphDatabase\\Proteins\\Proteins\\part3_Proteins\\Proteins\\query\\rattus_norvegicus_2KHZ.32.sub.grf");


//            SearchSpace is a Map<Integer,List<nodes>>
//            CompleteSolution is a ArrayList<Map<Integer,node>>
//            For Query,data structure is Map<Integer,Query> query
            try
            {
                GraphDatabaseService db = new GraphDatabaseFactory()
                        .newEmbeddedDatabaseBuilder(new File("C:\\Protein\\backbones_1CJF.grf"))
                        .setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
                        .setConfig(GraphDatabaseSettings.string_block_size, "60")
                        .setConfig(GraphDatabaseSettings.array_block_size, "300")
                        .newGraphDatabase();

                Map<Integer, ArrayList<Node>> phi = getSearchSpace(query,db);

                //Use DFS to find the search Order and store in a global string processingOrder and then do the string manipulations to store its value in int[] SearchOrder
                String str=DFS(query);
                processingOrder=processingOrder.substring(0,processingOrder.length()-1);
                String[] SearchIndex=processingOrder.split("-");


                String storeProcessingOrder=processingOrder;
                int[] SearchOrder=new int[SearchIndex.length];

                //Can print SearchOrder as well

                System.out.println("Print SearchOrder using DFS");
                //
                for(int i=0;i<SearchIndex.length;i++)
                {
                    SearchOrder[i]=Integer.parseInt(SearchIndex[i]);
                }
                System.out.println(Arrays.toString(SearchOrder));
                 Map<Integer, ArrayList<Node>> neighor =neighbourProfile(query,db);

                processingOrder="";

                //Get the best search order
                //Use ComputationalOrder;
                Set<Integer> proc=new HashSet();
                getBestSearchOrder(query,0,proc,phi);
                System.out.println("Best Search order"+ComputationalOrder.toString());

                Arrays.sort(SearchOrder);


                System.out.println("Printing for GraphQL matching");
                long startTimeForGraphQL=System.currentTimeMillis();
                refineSearchSpace(10, query, neighor, db);
                Search(0,SearchOrder,query,db, neighor);
                long endTimeForGraphQL=System.currentTimeMillis();
                System.out.println("(endTimeForGraphQL-startTimeForGraphQL)"+(endTimeForGraphQL-startTimeForGraphQL));


                db.shutdown();




            }catch(Exception e)
            {
                e.printStackTrace();
            }


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

}

