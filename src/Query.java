import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * This class contains vertex ID,its label and set of edges.
 *
 */
public class Query
{
    int ID;//for v 0 1 the ID is 0
    String Label;//for v 0 1 the Label is 1
    Set<Integer> edges;
    public Query()
    {

        ID=0;
        edges=new HashSet<>();


    }

    public Set<Integer> getEdges()
    {
        return this.edges;
    }

    public int isEmpty()
    {
        return this.edges.size();
    }

    public void setID(int ID)
    {
        this.ID=ID;
    }

    public int getID()
    {
        return this.ID;

    }

    public void setLabel(String label)
    {
        this.Label=label;
    }

    public String getLabel()
    {
        return this.Label;
    }

    public String toString()
    {

        return "ID="+this.ID+" Label="+this.Label+" Edges={"+this.getEdges().toString()+"}";
    }
}
