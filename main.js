/*jslint node: true, plusplus: true, nomen: true */
"use strict";

var twitterAccessToken = "22259918-eQDMgqyLtsOPhY7FYBA3EFauNe7TYkAcvAqVhVM",
    twitterAccessTokenSecret = "SjSoRzuuFur9qlbXZleahnDK8SFsXuOgeYAIdQuKA",
    twitterConsumerKey = "DvURr4Q6Kr6sswUvvLzA",
    twitterConsumerSecret = "a7gHEWLsdjxD9fX9fHdoJ4QlukRTqzsrTPwhWdpiNmM",
    mongoConnectionString = "mongodb://cdibox.volgy.com:27017/vu_cs103";

var MongoClient = require('mongodb').MongoClient;
var userCollection, friendshipCollection;

var OAuth = require('oauth/lib/oauth').OAuth;
var oa = new OAuth(
    "http://twitter.com/oauth/request_token",
    "http://twitter.com/oauth/access_token",
    twitterConsumerKey,
    twitterConsumerSecret,
    "1.0A",
    null,
    "HMAC-SHA1"
);

var users = {};
var unexploredUsers = [];
var exploringUser;

function dbInsertCallback(error, coll) {
    if (error) {
        console.log(error);
        throw error;
    }
}

function processResponse(error, data, response) {
    var i, now, limitReset, limitRemaining, limitLimit,
        limitTimeRemaining, nextRequestTime,
        user, dbCollection;

    now = new Date();

    if (error) {
        console.log(require('sys').inspect(error));
        throw error;
    } else {
        data = JSON.parse(data);

        for (i = 0; i < data.users.length; i++) {
            user = data.users[i];
            if (users[user.id_str] === undefined) {
                users[user.id_str] = user;
                userCollection.insert(user, dbInsertCallback);
                unexploredUsers.push(user);
            }
            friendshipCollection.insert({'src': (exploringUser ? exploringUser.id_str : undefined), 'dst': user.id_str}, dbInsertCallback);
            console.log((exploringUser ? exploringUser.screen_name : "me") + "--->" + user.screen_name);
        }

        if (!data.next_cursor) {
            exploringUser = unexploredUsers.shift();
            if (!exploringUser) {
                console.log("## Exploration finished. Found " + users.length + " users.");
                return;
            }
            data.next_cursor = undefined;   // make sure it is not 0
        }

        // Rate limiting
        limitReset = parseInt(response.headers['x-rate-limit-reset'], 10);
        limitRemaining = parseInt(response.headers['x-rate-limit-remaining'], 10);
        limitLimit = parseInt(response.headers['x-rate-limit-limit'], 10);
        limitTimeRemaining = (limitReset * 1e3) - now.getTime();
        if (limitRemaining === 0) {
            nextRequestTime = limitTimeRemaining + 5000; // to be on the safe side
        } else {
            nextRequestTime = Math.round(limitTimeRemaining / limitRemaining);
        }
        console.log("## Limit: " + limitRemaining + " / " + limitLimit +
            " requests (reset in " + Math.round(limitTimeRemaining / 1e3) + " s). Next request in " +
            nextRequestTime + " ms.");

        setTimeout(function () {
            exploreUser(exploringUser ? exploringUser.id_str : undefined, data.next_cursor);
        }, nextRequestTime);
    }
}

function exploreUser(user_id, cursor) {
    var queryString = "https://api.twitter.com/1.1/friends/list.json?skip_status=true&include_user_entities=false";
    if (user_id) {
        queryString += "&user_id=" + user_id;
    }
    if (cursor) {
        queryString += "&cursor=" + cursor;
    }
    oa.get(
        queryString,
        twitterAccessToken,
        twitterAccessTokenSecret,
        processResponse
    );
}

MongoClient.connect(mongoConnectionString, function (err, db) {
    if(err) { return console.log(err); }
    userCollection = db.collection('Users');
    friendshipCollection = db.collection('Friendships');
    exploreUser();
});



