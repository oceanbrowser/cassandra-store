"use strict";

/**
 * Return the `cassandraStore` extending `express`' session Store.
 *
 * @param {Object} session
 * @return {function}
 * @api public
 */
module.exports = function (session)
{
    /**
     * Dependencies.
     */
    var util = require("util");
    var _ = require("underscore");
    var debug = require("debug")("cassandra-store");
    var Queries = require("./queries");

    /**
     * Default variables.
     */
    var TABLE = "sessions";
    var defaultOptions = {
        "contactPoints": [ "localhost" ],
        "keyspace": "tests",
        "protocolOptions": {
            "port": 9042
        },
        "socketOptions": {
            "connectTimeout": 5000
        },
        "queryOptions": {
            "fetchSize": 5000,
            "autoPage": true,
            "prepare": true
        },
        "authProvider": {
            "username": "",
            "password": ""
        },
        // Time To Live
        "ttl": 86400, // Default TTL of 1 day
        
        // New default cookie options
        "cookieOptions": {
            "secure": true,
            "httpOnly": true,
            "sameSite": "strict"
        }
    };

    /**
     * Express' session Store.
     */
    var Store = session.Store;

    /**
     * Initialize CassandraStore with the given `options`.
     *
     * @param {Object} options
     * @api public
     */
    function CassandraStore(options)
    {
        options = _.extend({}, defaultOptions, options);
        Store.call(this, options);
        this.options = options;
        if (options.client) {
            this.client = options.client;
            delete options.client;
        }
        else {
            this.client = new require("cassandra-driver").Client(options);
        }
        if(options.table) {
            TABLE = this.options.table;
        }
        if(this.options.keyspace)
        {
            TABLE = this.options.keyspace + "." + TABLE;
        }
        
        // Determine if we're in development mode
        this.isDev = options.isDevelopment || false;

        // Set cookie options based on environment
        this.cookieOptions = this.isDev ? {
            secure: false,
            httpOnly: false,
            sameSite: 'lax',
            domain: 'dev.ob3.io',
            ...options.devCookieOptions,
        } : {
            ...this.options.cookieOptions,
            ...options.cookieOptions,
        };
        
        debug("Database configuration: ", JSON.stringify(this.options, null, 0));
        debug("Database table: ", TABLE);
        debug("Cookie options: ", JSON.stringify(this.cookieOptions, null, 0));
        debug("Running in " + (this.isDev ? "development" : "production") + " mode");
        
        this.client.on("log", function(level, className, message, furtherInfo)
        {
            debug("%s [%s]: %s (%s)", className, level, message, furtherInfo);
        });
        this.client.connect(function (error)
        {
            if (error)
            {
                debug("Database not available: " + error.message);
            }
            else
            {
                debug("Database store initialized");
            }
        });
    }

    /**
     * Inherit from `Store`.
     */
    util.inherits(CassandraStore, Store);

    /**
     * Attempt to fetch session by the given `sid`.
     *
     * @param {string} sid
     * @param {function} fn
     * @api public
     */
    CassandraStore.prototype.get = function (sid, fn)
    {
        var query = util.format(Queries.SELECT, TABLE, sid);
        debug("Query: %s", query);
        this.client.execute(query, this.options.queryOptions,
            function (error, result)
        {
            var sess = null;
            if (error)
            {
                debug("Session %s cannot be fetched: %s", sid, error);
            }
            else
            {
                debug("Session %s fetched", sid);
                debug("Select result: %s", JSON.stringify(result, null, 0));
                try
                {
                    if (result && result.rows)
                    {
                        if (result.rows.length > 0 && result.rows[0]["sobject"])
                        {
                            sess = JSON.parse(result.rows[0]["sobject"]);
                        }
                    }
                    debug("Session %s obtained", JSON.stringify(sess, null, 0));
                }
                catch (err)
                {
                    debug("Session %s cannot be parsed: %s", sid, err.message);
                }
            }
            return fn(error, sess);
        });
    };

    /**
     * Commit the given `session` object associated with the given `sid`.
     *
     * @param {string} sid
     * @param {Object} sess
     * @param {function} fn
     * @api public
     */
    CassandraStore.prototype.set = function (sid, sess, fn)
    {
        // Apply cookie options
        sess.cookie = {
            ...sess.cookie,
            ...this.cookieOptions,
        };
        
        var sobject = JSON.stringify(sess, null, 0);

    
        var ttl = sess.cookie.maxAge ? Math.round(sess.cookie.maxAge / 1000) : 0;
        if (isNaN(ttl) || ttl <= 0) {
            ttl = this.options.ttl || 86400; // Default to 1 day if not set or invalid
        }

        var query = util.format(Queries.UPDATE, TABLE, ttl, sobject, sid);
        debug("Query: %s", query);
        this.client.execute(query, this.options.queryOptions,
            function (error, result)
        {
            if (error)
            {
                debug("Session %s cannot be created: %s", sid, error);
            }
            else
            {
                debug("Session %s added", sid);
                debug("Update result: %s", JSON.stringify(result, null, 0));
            }
            return fn(error, result);
        });
    };

    /**
     * Destroy the session associated with the given `sid`.
     *
     * @param {string} sid
     * @param {function} fn
     * @api public
     */
    CassandraStore.prototype.destroy = function (sid, fn)
    {
        var query = util.format(Queries.DELETE, TABLE, sid);
        debug("Query: %s", query);
        this.client.execute(query, this.options.queryOptions,
            function (error, result)
        {
            if (error)
            {
                debug("Session %s cannot be deleted: %s", sid, error);
            }
            else
            {
                debug("Session %s deleted", sid);
                debug("Delete result: %s", JSON.stringify(result, null, 0));
            }
            return fn(error, result);
        });
    };

    return CassandraStore;
};