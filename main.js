/*jslint node: true, plusplus: true, nomen: true */
"use strict";

var options = {
        twitterAccessToken: "1092955122-x8ViAZ2030vjski3ZIvwqX6ZjAoqilqzvbfaQd9",
        twitterAccessTokenSecret: "PKLGIiHWA3mA1K2bfyVtynRyTeFy8xYwRmr6XhBGQ",
        twitterConsumerKey: "rdDcbK0Hqjd8ZcwxMmVg",
        twitterConsumerSecret: "oCoqcLBMeX65C4x4i0Xpj9mOe8Cdn5HOvlyaMLHCUY",
        mongoConnectionString: "mongodb://cdibox.volgy.com:27017/vu_cs103_feb21_w",
        maxDistance: 2,
        max_follows : 15000,
        max_followers : 15000,
        seedUser: 'VUCS103'
    };

var oauth = require('oauth/lib/oauth');
var querystring = require('querystring');
var MongoClient = require('mongodb').MongoClient;
var step = require('step');

var twitter = new oauth.OAuth(
        "http://twitter.com/oauth/request_token",
        "http://twitter.com/oauth/access_token",
        options.twitterConsumerKey,
        options.twitterConsumerSecret,
        "1.0A",
        null,
        "HMAC-SHA1"
    );

var userColl, tweetColl, followColl;

var stats = {
    nUsers: 0,
    nTweets: 0,
    nFollows: 0
};

function showStats() {
    console.log("Users: %d [%d (%ds)]  | Tweets:  %d [%d (%ds)] | Follows: %d [%d (%ds)] [%d (%ds)]", 
        stats.nUsers, userWQ.queue.length, userWQ.limitInterval / 1e3, 
        stats.nTweets, tweetWQ.queue.length, tweetWQ.limitInterval / 1e3, 
        stats.nFollows, followsWQ.queue.length, followsWQ.limitInterval / 1e3, followersWQ.queue.length, followersWQ.limitInterval / 1e3);
}

// Generic Work Queue
// This is a class, concrete work queue implementations
// should define buildQuery and processResults methods
function WorkQueue() {
    this.queue = [];
    this.idle = true;
    this.limitInterval = 0;
}

WorkQueue.prototype.crank = function () {
    var self = this, query;

    if (this.queue.length === 0) {
        self.idle = true;
        return;
    }

    query = self.buildQuery();

    self.idle = false;

    function processResponse(error, data, response) {
        if (error) {
            console.error("\n\n" + query.toString());
            switch (error.statusCode) {  

            // Intentional fallthrough: retry error cases
            case 420: // Enhance Your Calm
            case 429: // Too Many Requests
            case 503: // Service Unavailable
            case 504: // Gateway timeout
                self.limitInterval += 60e3; // increase the the interval by 1 minute (play safe)
                setTimeout(submitQuery, self.limitInterval);
                console.error("Retrying....");
                break;

            // Silent fail for all other problems
            default:
                self.crank();
                console.error("Skipping....\n\n");
                break;
            }
            console.error("\n\n");
            return;
        }

        data = JSON.parse(data);
        self.processResults(data, query);
        self.limitInterval = self.calcInterval(response.headers);
        self.crank();
    }

    function submitQuery() {
        twitter.get(query.toString(), options.twitterAccessToken, options.twitterAccessTokenSecret, processResponse);
    }

    setTimeout(submitQuery, self.limitInterval);
};

WorkQueue.prototype.enqueue = function (workItems) {
    this.queue = this.queue.concat(workItems);
    if (this.idle) {
        this.crank();
    }
};

WorkQueue.prototype.calcInterval = function (headers) {
    var now, limitReset, limitRemaining, limitLimit, limitTimeRemaining;

    now = new Date();
    limitReset = parseInt(headers['x-rate-limit-reset'], 10);
    limitRemaining = parseInt(headers['x-rate-limit-remaining'], 10);
    limitLimit = parseInt(headers['x-rate-limit-limit'], 10);
    limitTimeRemaining = (limitReset * 1e3) - now.getTime();
    if (limitRemaining === 0) {
        return (limitTimeRemaining + 10e3); // to be on the safe side (10s timesync error)
    }
    return Math.round(limitTimeRemaining / limitRemaining);
};

// User Work Queue - for user lookup
// work items: <user_id>
var userWQ = new WorkQueue();

userWQ.buildQuery = function () {
    return {
        params: {user_id: this.queue.splice(0, 100).join(',')}, // Twitter limit (100 users/request)
        toString: function () {
            return ("https://api.twitter.com/1.1/users/lookup.json?" +
                querystring.stringify(this.params));
        }
    };
};

userWQ.processResults = function (data, query) {
    data.forEach(function (user) {
        user._id = user.id_str; // Use the same (unique) id in mongo
        user.distance = knownUsers[user.id_str];
        userColl.save(user, {w: 1}, function (err, result) {
            if (err) {
                console.error(err);
            }
        });
        if (user.friends_count < options.max_follows) {
            followsWQ.enqueue([{user_id: user.id_str}]);
        }
        if (user.followers_count < options.max_followers) {
            followersWQ.enqueue([{user_id: user.id_str}]);
        }
    });
    stats.nUsers += data.length;
    showStats();
};

// Tweet Work Queue - for getting all tweets of a user
// work items: {user_id: <user_id>, max_id: <max_id>}
//                                  ^ ~ optional ~ ^
var tweetWQ = new WorkQueue();

tweetWQ.buildQuery = function () {
    return {
        params: this.queue.shift(),
        toString: function () {
            return ("https://api.twitter.com/1.1/statuses/user_timeline.json?count=200&trim_user=true&" +
                querystring.stringify(this.params));
        }
    };
};

tweetWQ.processResults = function (data, query) {
    var lastTweet;

    data.forEach(function (tweet) {
        tweet._id = tweet.id_str; // Use the same (unique) id in mongo
        tweetColl.save(tweet, {w: 1}, function (err, result) {
            if (err) {
                console.error(err);
            }
        });
    });
    stats.nTweets += data.length;
    showStats();

    if (data.length) {
        lastTweet = data[data.length - 1];
        this.enqueue([{user_id: query.params.user_id, max_id: lastTweet.id_str}]);
    }
};

// Follows Work Queue - for getting all users followed by this user
// All discovered users will be automatically added to the discovery set
// work items: {user_id: <user_id>, cursor: <cursor>}
//                                  ^ ~ optional ~ ^
var followsWQ = new WorkQueue();

followsWQ.buildQuery = function () {
    return {
        params: this.queue.shift(),
        toString: function () {
            return ("https://api.twitter.com/1.1/friends/ids.json?" +
                querystring.stringify(this.params));
        }
    };
};

followsWQ.processResults = function (data, query) {
    var srcUserId;

    if (!data.ids) {
        return;
    }

    srcUserId = query.params.user_id;
    data.ids.forEach(function (dstUserId) {
        followColl.save({src: srcUserId, dst: dstUserId.toString()}, {w: 1}, function (err, result) {
            if (err) {
                console.error(err);
            }
        });
    });

    stats.nFollows += data.ids.length;
    showStats();

    if (data.next_cursor) {
        this.enqueue([{user_id: srcUserId, cursor: data.next_cursor_str}]);
    }

    addUsers(data.ids, knownUsers[srcUserId] + 1);
};

// Followers Work Queue - for getting all users follow this user
// All discovered users will be automatically added to the discovery set
// work items: {user_id: <user_id>, cursor: <cursor>}
//                                  ^ ~ optional ~ ^
var followersWQ = new WorkQueue();

followersWQ.buildQuery = function () {
    return {
        params: this.queue.shift(),
        toString: function () {
            return ("https://api.twitter.com/1.1/followers/ids.json?" +
                querystring.stringify(this.params));
        }
    };
};

followersWQ.processResults = function (data, query) {
    var dstUserId;

    if (!data.ids) {
        return;
    }

    dstUserId = query.params.user_id;
    data.ids.forEach(function (srcUserId) {
        followColl.save({src: srcUserId.toString(), dst: dstUserId}, {w: 1}, function (err, result) {
            if (err) {
                console.error(err);
            }
        });
    });

    stats.nFollows += data.ids.length;
    showStats();

    if (data.next_cursor) {
        this.enqueue([{user_id: dstUserId, cursor: data.next_cursor_str}]);
    }

    addUsers(data.ids, knownUsers[dstUserId] + 1);
};

// Adding new users to be processed
// Queues are fed from here (and by themselves)
var knownUsers = {};

function addUsers(userIds, distance) {
    var i, userId,
        userItems = [],
        tweetItems = [];
        // followsItems = [],
        // followersItems = [];

    if (distance > options.maxDistance) {
        return;
    }

    for (i = 0; i < userIds.length; i++) {
        userId = userIds[i];
        if (!(userId in knownUsers)) {
            knownUsers[userId] = distance;
            userItems.push(userId);
            tweetItems.push({user_id: userId});
            // followsItems.push({user_id: userId});
            // followersItems.push({user_id: userId});
        }
    }

    userWQ.enqueue(userItems);
    tweetWQ.enqueue(tweetItems);
    // followsWQ.enqueue(followsItems);
    // followersWQ.enqueue(followersItems);
}

// Seeding the crawl process with a wildcard search
// for root user(s)
function seedSearch(username) {
    var userIds, qString;

    function seedResults(error, data, response) {
        var users;

        if (error) {
            console.error(error, response);
            return;
        }

        users = JSON.parse(data);
        userIds = [];
        console.log("Seeding the database with the following user(s):");
        users.forEach(function (user) {
            console.log("%s [id:%s], Name: '%s'", user.screen_name, user.id_str, user.name);
            userIds.push(user.id_str);
        });
        addUsers(userIds, 0);
    }

    // Fuzzy search
    // qString = "https://api.twitter.com/1.1/users/search.json?q=" + username;

    // Simple lookup (exact match)
    qString = "https://api.twitter.com/1.1/users/lookup.json?screen_name=" + username;

    twitter.get(
        qString,
        options.twitterAccessToken,
        options.twitterAccessTokenSecret,
        seedResults
    );
}

// Primitive command line processing
if (process.argv.length > 2) {
    options.seedUser = process.argv[2];
}


// Connect to db, get all connections, start the crawler
step(
    function connectDb() {
        MongoClient.connect(options.mongoConnectionString, {w: 1, maxPoolSize: 1}, this);
    },
    function fetchCollections(err, db) {
        if (err) {
            console.error(err);
            throw err;
        }
        db.collection("User", this.parallel());
        db.collection("Tweet", this.parallel());
        db.collection("Follow", this.parallel());
    },
    function startCrawler(err, uColl, tColl, fColl) {
        if (err) {
            console.error(err);
            throw err;
        }
        userColl = uColl;
        tweetColl = tColl;
        followColl = fColl;

        seedSearch(options.seedUser);
    }
);
