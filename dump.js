/*jslint node: true, plusplus: true, nomen: true */
"use strict";

var options = {
        mongoConnectionString: "mongodb://cdibox.volgy.com:27017/vu_cs103_trunc_d2",
    };

var MongoClient = require('mongodb').MongoClient;

var userColl, tweetColl, followColl;


// Connect to db, get all connections, start the crawler
MongoClient.connect(options.mongoConnectionString, function (err, db) {
    if (err) {
        console.error(err);
        return;
    }
    db.collection("User", function (err, coll) {
        if (err) {
            console.error(err);
            return;
        }
        userColl = coll;
        db.collection("Tweet", function (err, coll) {
            if (err) {
                console.error(err);
                return;
            }
            tweetColl = coll;
            db.collection("Follow", function (err, coll) {
                if (err) {
                    console.error(err);
                    return;
                }
                followColl = coll;
                dump();
            });
        });
    });
});
