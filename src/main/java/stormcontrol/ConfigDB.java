package stormcontrol;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

public final class ConfigDB {
	private MongoClient mongoClient;

	public ConfigDB() {
		// where do I get this from?
		String connection_string = "mongodb://webmonitor:42RKBu2QyeOUHkxOdHAhjfIpw1cgIQVgViO4U4nPr0s=@10.4.73.172:27010/admin";
		connect(connection_string);
	}

	public MongoCollection<Document> check(String db, String collection_name) {
		String experiment_name = "testing";
		String db_name = experiment_name + "_" + db;
		MongoDatabase database = mongoClient.getDatabase(db_name);
		return database.getCollection(collection_name);
	}

	public void connect(String conn_str) {
		ConnectionString connection_string = new ConnectionString(conn_str);
		MongoClientSettings settings = MongoClientSettings.builder()
				.applyConnectionString(connection_string).retryWrites(true).build();
		mongoClient = MongoClients.create(settings);
	}

	public Document read(String db_name, String collection_name, Bson filter) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		return collection.find(filter).first();
    }
	public Document read(String db_name, String collection_name) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		return collection.find().first();
	}

	public void writeOne(String db_name, String collection_name, Document log) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		collection.insertOne(log);
	}
}
