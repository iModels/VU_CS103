/*jslint node: true, plusplus: true, nomen: true */
"use strict";

var options = {
        mongoConnectionString: "mongodb://cdibox.volgy.com:27017/vu_cs103_feb20"
    };

var MongoClient = require('mongodb').MongoClient;

function sanitize(str) {
    return str.replace(/\'/gi, "''").replace(/[\x00-\x20]/gi, " ");
}

function dump(userColl, tweetColl, followColl) {
    var userCnt, tweetCnt;

    userCnt = 1;
    userColl.find().each(function (err, user) {
        if (err) {
            console.error(err);
            return;
        }
        if (!user) {
            console.warn("WARNING: empty user object");
            return;
        }
        if ((userCnt % 1e3) === 0) {
            console.warn("Processed %d users", userCnt);
        }
        user.seq = userCnt++;
        user.id_str && console.log("users(%d).id = %s;", user.seq, user.id_str);
        user.screen_name && console.log("users(%d).screen_name = '%s';", user.seq, user.screen_name);
        user.name && console.log("users(%d).name = '%s';", user.seq, sanitize(user.name));
        user.created_at && console.log("users(%d).created_at = '%s';", user.seq, user.created_at);
        user.time_zone && console.log("users(%d).time_zone = '%s';", user.seq, user.time_zone);

        followColl.find({src: user.id_str}).toArray(function (err, follows) {
            follows.length && console.log("users(%d).follows = [%s];", user.seq, follows.map(function (f) {return f.dst;}).join(","));
        });

        followColl.find({dst: user.id_str}).toArray(function (err, followers) {
            followers.length && console.log("users(%d).followers = [%s];", user.seq, followers.map(function (f) {return f.src;}).join(","));
        });
    });

    tweetCnt = 1;
    tweetColl.find().each(function (err, tweet) {
        if (err) {
            console.error(err);
            return;
        }
        if (!tweet) {
            console.warn("WARNING: empty tweet object");
            return;
        }
        if ((tweetCnt % 10e3) === 0) {
            console.warn("Processed %d tweets", tweetCnt);
        }
        tweet.seq = tweetCnt++;
        tweet.user && console.log("tweets(%d).user = %s;", tweet.seq, tweet.user.id_str);
        tweet.created_at && console.log("tweets(%d).created_at = '%s';", tweet.seq, tweet.created_at);
        tweet.text && console.log("tweets(%d).text = '%s';", tweet.seq, sanitize(tweet.text));
        console.log("tweets(%d).is_retweet = %s;", tweet.seq, tweet.retweeted_status ? "true" : "false");
        tweet.entities && tweet.entities.hashtags && tweet.entities.hashtags.length && 
            console.log("tweets(%d).hashtags = [%s];", tweet.seq, tweet.entities.hashtags.map(function (t) {return "'" + t.text + "'";}).join(","));
        tweet.entities && tweet.entities.user_mentions && tweet.entities.user_mentions.length && 
            console.log("tweets(%d).mentions = [%s];", tweet.seq, tweet.entities.user_mentions.map(function (m) {return m.id_str;}).join(","));
        tweet.coordinates && console.log("tweets(%d).coordinates = [%s];", tweet.seq, tweet.coordinates.coordinates.toString());
    });
}

// Connect to db, get all connections, start the crawler
MongoClient.connect(options.mongoConnectionString, function (err, db) {
    if (err) {
        console.error(err);
        return;
    }
    db.collection("User", function (err, userColl) {
        if (err) {
            console.error(err);
            return;
        }
        db.collection("Tweet", function (err, tweetColl) {
            if (err) {
                console.error(err);
                return;
            }
            db.collection("Follow", function (err, followColl) {
                if (err) {
                    console.error(err);
                    return;
                }
                dump(userColl, tweetColl, followColl);
            });
        });
    });
});
