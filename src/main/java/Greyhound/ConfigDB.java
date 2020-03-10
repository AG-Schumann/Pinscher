package Greyhound;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public final class ConfigDB {
	private MongoClient mongoClient;
	private String experiment_name;

	public ConfigDB(String connection_uri, String experiment_name) {
		this.experiment_name = experiment_name;
		connect(connection_uri);
	}

	public MongoCollection<Document> check(String db, String collection_name) {
		String db_name = experiment_name + "_" + db;
		MongoDatabase database = mongoClient.getDatabase(db_name);
		return database.getCollection(collection_name);
	}

	public void connect(String conn_str) {
		ConnectionString connection_string = new ConnectionString(conn_str);
		MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connection_string)
				.retryWrites(true).build();
		mongoClient = MongoClients.create(settings);
	}

	public Document readOne(String db_name, String collection_name, Bson filter) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		return collection.find(filter).first();
	}

	public Document readOne(String db_name, String collection_name) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		return collection.find().first();
	}

	public List<Document> readMany(String db_name, String collection_name, Bson filter) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		MongoCursor<Document> cursor = collection.find(filter).iterator();
		List<Document> list = new ArrayList<Document>();
		try {
			while (cursor.hasNext()) {
				list.add(cursor.next());
			}
		} catch (Exception e) {
		}
		return list;
	}

	public List<Document> readMany(String db_name, String collection_name) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		MongoCursor<Document> cursor = collection.find().iterator();
		List<Document> list = new ArrayList<Document>();
		try {
			while (cursor.hasNext()) {
				list.add(cursor.next());
			}
		} catch (Exception e) {
		}
		return list;
	}

	public void writeOne(String db_name, String collection_name, Document doc) {
		MongoCollection<Document> collection = check(db_name, collection_name);
		collection.insertOne(doc);
	}
	
	public void log(String msg, int level) {
		
		StackTraceElement[] ste = Thread.currentThread().getStackTrace(); 
		Document doc = new Document("msg", msg);
		doc.append("level", level);
		doc.append("name", ste[0]);
		doc.append("funcname", ste[5]);
		doc.append("lineno", ste[4]);
		writeOne("logging", "logs", doc);
	}
}
