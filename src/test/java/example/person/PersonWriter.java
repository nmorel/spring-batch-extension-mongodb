package example.person;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

/** Simple writer */
public class PersonWriter implements ItemWriter<Person>
{
    private final DB db;

    public PersonWriter( DB db )
    {
        this.db = db;
    }

    @Override
    public void write( List<? extends Person> items ) throws Exception
    {
        DBCollection collection = db.getCollection(Person.class.getSimpleName());
        for( Person person : items )
        {
            collection.save(BasicDBObjectBuilder.start().add("firstName", person.getFirstName()).add("lastName", person.getLastName()).get());
        }
    }
}
