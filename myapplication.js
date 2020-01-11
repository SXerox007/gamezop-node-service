const express = require('express')
const bodyParser = require('body-parser')
const redis = require('redis');
const kafka = require('kafka-node');
var mongo = require('mongodb');

// Create a new instance of express
const app = express()

// Tell express to use the body-parser middleware and to not parse extended bodies
app.use(bodyParser.urlencoded({ extended: false }))

//mongo client
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/gamezop";

MongoClient.connect(url, function(err, db) {
  if (err) throw err;
  	console.log("Mongo Database connected with success!");
  	createGamezopCollection();
  	db.close();
});

// this creates a new client
const redisClient = redis.createClient(); 

// Redis
redisClient.on('connect', function () {
    console.log('Redis connected with success.');
});
redisClient.on('error', function (err) {
    console.log('Error in connection Redis:' + err);
});


// kafka consumer impl
try {
  const Consumer = kafka.Consumer;
 const client = new kafka.KafkaClient({idleConnection: 24 * 60 * 60 * 1000,  kafkaHost: 'localhost:9092'});

 let consumer = new Consumer(
    client,
    [{ topic: 'gamezop', partition: 0 }],
    {
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
    }
  );
  consumer.on('message', function(message) {
	console.log('Kafka Said New Player Name:',message.value);
	// get player info from redis
	redisClient.get(message.value, function(err, reply) {
		if (err) throw err;
    	console.log("Player Info from Redis:",reply);
		// write into mongodb or sql
    	insertGamezopCollection(JSON.parse(reply),message.value)

	});
  })
  consumer.on('error', function(error) {
    //  handle error 
    console.log('Error:', error);
  });
}
catch(error) {
  // catch error trace
  console.log(error);
}


// insert into mongodb
var insertGamezopCollection = function(playerInfo,redisKey){
	MongoClient.connect(url, function(err, db) {
	if (err) throw err;
			var dbo = db.db("gamezop");
			var data = playerInfo;
			dbo.collection("all_players").insertOne(data, function(err, res) {
			if (err) throw err;
			console.log("Player Info insert with success.");
			// remove the data from redis
			deletePlayerInfo(redisKey);
			db.close();
		});
	});

};

// remove the new player data from redis
var deletePlayerInfo = function(key){
	redisClient.del(key, function(err, reply) {
		if (err) throw err;
    	console.log("Delete the player info from redis with success.",reply);
	});
}

//create collection
var createGamezopCollection =  function(){
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;
	  var dbo = db.db("gamezop");
	  dbo.createCollection("all_players", function(err, res) {
	    if (err) throw err;
	    console.log("all_players Collection created!");
	    db.close();
	  });
	});
};

// get the all players details
app.get('/game/gamezop',function(req,res){
	//get all gamezop players info
		MongoClient.connect(url, function(err, db) {
		if (err) throw err;
			var dbo = db.db("gamezop");
			dbo.collection("all_players").find().toArray(function(err, result) {
		if (err) throw err;
				res.set('Content-Type', 'application/json')
				res.send(JSON.stringify(result));
			db.close();
		});
	});

});


// Tell our app to listen on port 3000
app.listen(3000, function (err) {
  if (err) {
    throw err
  }

  console.log('Server started on port 3000')
})