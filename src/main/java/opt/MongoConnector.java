package opt;

import com.mongodb.client.MongoDatabase;

/**
 * Created by jim on 29/5/2017.
 */
public interface MongoConnector<T extends Number> {
    void insertDocument(String id, T pressureValue, long timestamp);
    MongoDatabase connectToDB();
}