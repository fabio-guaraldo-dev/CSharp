using System;
using System.Collections.Generic;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Threading.Tasks;

namespace MongoDbTest
{
    class Program
    {
        // register in https://www.mongodb.com/blog/post/quick-start-c-sharp-and-mongodb-starting-and-setup
        // replace <database>:<password> values
        private static MongoClient mongoClient = new MongoClient("mongodb+srv://<database>:<password>@cluster0.6yaog.mongodb.net?retryWrites=true&w=majority");
        

        /*
         Structure sample for database: sample_training, collection: grades
         {
	        "_id": ObjectId("56d5f7eb604eb380b0d8d9c6"),
	        "student_id": 24.0,
	        "scores": [
                {
		            "type": "exam",
		            "score": 99.991816279874541
	            }, {
		            "type": "quiz",
		            "score": 84.876920058583011
	            }, {
		            "type": "homework",
		            "score": 52.0506541636897
	            }, {
		            "type": "homework",
		            "score": 88.874853747751231
	            }
            ],
	        "class_id": 82.0
        }
        */

        static async Task Main(string[] args)
        {
            //ListDb();
            //CreateDocument();
            //DisplayFirstDocument();
            //DiplayDocumentByStudentId(10000);
            //DiplayDocumentByStudentId(50000);
            //DisplayAllDocuments();
            //DisplayByScoreGreaterThan(99.99M);
            //DisplayLongListUsingCursor(70M);
            //await DisplayLongListUsingForeachAsync(70M);
            //await DisplayBySortStudentIdAsync(99.99M);
            //UpdateOneStudent(10000);
            await CreateCollectionAsync();
            Console.ReadKey();
        }

        static void ListDb()
        {
            var dblist = mongoClient.ListDatabases().ToList();
            Console.WriteLine("The list of databases on this server is: ");
            foreach (var db in dblist)
            {
                Console.WriteLine(db);
            }
        }

        static IMongoCollection<BsonDocument> GetCollection(string database, string collection)
        {
            IMongoDatabase mongoDatabase = mongoClient.GetDatabase(database);
            return mongoDatabase.GetCollection<BsonDocument>(collection);
        }

        static void CreateDocument()
        {
            Console.WriteLine("Creating new document...");
            var document = new BsonDocument {
                {
                    "student_id", 10000
                },
                {
                    "scores", new BsonArray {
                                new BsonDocument {
                                    { "type", "exam" },
                                    { "score", 88.12334193287023 }
                                },
                                new BsonDocument {
                                    { "type", "quiz" },
                                    { "score", 74.92381029342834 }
                                },
                                new BsonDocument {
                                    { "type", "homework" },
                                    { "score", 89.97929384290324 }
                                },
                                new BsonDocument {
                                    { "type", "homework" },
                                    { "score", 82.12931030513218 }
                                }
                            }
                },
                { "class_id", 480 }
            };

            var collection = GetCollection("sample_training", "grades");
            collection.InsertOne(document);
        }

        static void DisplayFirstDocument()
        {
            Console.WriteLine("Displaying first document...");
            var collection = GetCollection("sample_training", "grades");
            var firstDocument = collection.Find(new BsonDocument()).FirstOrDefault();
            Console.WriteLine(firstDocument.ToString());
        }

        static void DiplayDocumentByStudentId(int student_id)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("student_id", student_id);
            Console.WriteLine("Displaying document for student id...");
            var collection = GetCollection("sample_training", "grades");
            var firstDocument = collection.Find(filter).FirstOrDefault();
            Console.WriteLine(firstDocument?.ToString() ?? "No document found");
        }

        static void DisplayAllDocuments()
        {
            Console.WriteLine("Displaying all documents...");
            var collection = GetCollection("sample_training", "grades");
            var allDocuments = collection.Find(new BsonDocument()).ToList();
            foreach (BsonDocument doc in allDocuments)
            {
                Console.WriteLine(doc);
            }
        }

        static void DisplayByScoreGreaterThan(decimal value)
        {
            var filter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
                "scores", new BsonDocument
                {
                    { "type","exam" },
                        { "score", new BsonDocument { 
                            { "$gte", value } 
                        } 
                    }
                }
            );
            var collection = GetCollection("sample_training", "grades");
            var allDocuments = collection.Find(filter).ToList();
            foreach (BsonDocument doc in allDocuments)
            {
                Console.WriteLine(doc);
            }
        }

        static void DisplayLongListUsingCursor(decimal value)
        {
            // for long lists
            var filter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
                "scores", new BsonDocument
                {
                    { "type","exam" },
                        { "score", new BsonDocument {
                            { "$gte", value }
                        }
                    }
                }
            );
            var collection = GetCollection("sample_training", "grades");
            var allDocuments = collection.Find(filter).ToCursor();
            foreach (BsonDocument doc in allDocuments.ToEnumerable())
            {
                Console.WriteLine(doc);
            }
        }

        static async Task DisplayLongListUsingForeachAsync(decimal value)
        {
            // for long lists
            var filter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
                "scores", new BsonDocument
                {
                    { "type","exam" },
                        { "score", new BsonDocument {
                            { "$gte", value }
                        }
                    }
                }
            );
            var collection = GetCollection("sample_training", "grades");
            await collection.Find(filter).ForEachAsync(doc => Console.WriteLine(doc));
            
        }

        static async Task DisplayBySortStudentIdAsync(decimal value)
        {
            var filter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
                "scores", new BsonDocument
                {
                    { "type","exam" },
                        { "score", new BsonDocument {
                            { "$gte", value }
                        }
                    }
                }
            );
            var sort = Builders<BsonDocument>.Sort.Descending("student_id");
            var collection = GetCollection("sample_training", "grades");
            await collection.Find(filter).Sort(sort).ForEachAsync(doc => Console.WriteLine(doc));
        }

        static void UpdateOneStudent(int student_id)
        {
            DiplayDocumentByStudentId(student_id);
            var filter = Builders<BsonDocument>.Filter.Eq("student_id", student_id);
            var update = Builders<BsonDocument>.Update.Set("class_id", 483);
            var collection = GetCollection("sample_training", "grades");
            collection.UpdateOne(filter, update);
            DiplayDocumentByStudentId(student_id);
        }

        static async Task CreateCollectionAsync()
        {
            var database = mongoClient.GetDatabase("sample_training");
            await database.CreateCollectionAsync("my_collection");
            IMongoCollection<BsonDocument> collection = GetCollection("sample_training", "my_collection");
        }

    }
}
