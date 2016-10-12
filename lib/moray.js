/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

/*
 * Moray convenience and initialization functions
 *
 *
 * Migration of old records is performed by constructing the corresponding
 * model using the existing parameters stored in moray, then calling `raw()` to
 * get the new record to put into moray. Since migration only uses the new model
 * to construct new instances, you must be able to create new, valid records
 * from the parameters in the old records.
 *
 *
 * Migrating a bucket involves the following steps:
 * 1. Check and update bucket schema and version, if needed.
 * 2. Re-index objects. This needs to be done before re-putting objects,
 *    otherwise when new indexes are added, any existing values will be
 *    nullified when we get them from Moray.
 * 3. Re-put objects.
 *
 * Every step happens for each bucket every time FWAPI starts. Since FWAPI could
 * have crashed during re-indexing or re-putting, we run both each time to check
 * for any records that still need to be processed.
 */

'use strict';

var assert = require('assert-plus');
var async = require('async');
var clone = require('clone');
var mod_moray = require('moray');
var util = require('util');
var vasync = require('vasync');
var VError = require('verror');

var MAX_RETRIES = 5;


/*
 * Keeps repeating repeatCb, calling afterCb once done.
 * the arguments to repeatCb are: fn(err, res, keepGoing)
 * Every time repeatCb calls cb with keepGoing === true,
 * repeatCb will be called again.
 */
function repeat(repeatCb, afterCb) {
    function next(err, res, keepGoing) {
        if (!keepGoing) {
            afterCb(err, res);
            return;
        }

        setImmediate(repeatCb, next);
    }

    setImmediate(repeatCb, next);
}


// --- Internal


/*
 * Initialize a bucket in Moray if it doesn't exist yet. If the bucket already
 * exists and is an older version of the schema, update the schema. If the
 * schema is newer that what we have, log and do nothing.
 */
function putBucket(opts, callback) {
    var moray = opts.moray;
    var bucket = opts.bucket;
    var log = opts.log;

    var retries = 0;

    var schema = clone(bucket.schema);
    schema.options = schema.options || {};
    schema.options.version = bucket.version;

    repeat(function (next) {
        moray.getBucket(bucket.name, function (gErr, bucketObj) {
            if (gErr) {
                if (!VError.findCauseByName(gErr, 'BucketNotFoundError')) {
                    log.error(
                        { err: gErr, retries: retries, bucket: bucket.name },
                        'putBucket: error getting bucket');
                    if (retries >= MAX_RETRIES) {
                        next(gErr, null, false);
                    } else {
                        retries += 1;
                        setTimeout(next, 5000, null, null, true);
                    }
                    return;
                }

                moray.createBucket(bucket.name, schema, function (cErr) {
                    if (cErr) {
                        log.error({
                            err: cErr,
                            retries: retries,
                            bucket: bucket.name
                        }, 'putBucket: error creating bucket');

                        if (retries >= MAX_RETRIES) {
                            next(cErr, null, false);
                        } else {
                            retries += 1;
                            setTimeout(next, 5000, null, null, true);
                        }
                    } else {
                        log.info({ schema: schema, bucket: bucket.name },
                            'putBucket: created bucket');
                        next(null, null, false);
                    }
                });
                return;
            }

            var version =
                (bucketObj.options ? bucketObj.options.version : 0) || 0;

            if (bucket.version <= version) {
                var msg = bucket.version < version ?
                    'bucket has a newer schema; not updating' :
                    'bucket up to date';

                log.info({
                    bucket: bucket.name,
                    existing: version,
                    current: bucket.version
                }, 'putBucket: %s', msg);

                next(null, null, false);
                return;
            }

            log.info({ existing: bucketObj, current: bucket },
                'putBucket: updating bucket');

            moray.updateBucket(bucket.name, schema, function (uErr) {
                if (uErr) {
                    log.error(
                        { err: uErr, retries: retries, bucket: bucket.name },
                        'putBucket: error updating bucket');
                    if (retries >= MAX_RETRIES) {
                        next(uErr, null, false);
                    } else {
                        retries += 1;
                        setTimeout(next, 5000, null, null, true);
                    }
                    return;
                }

                log.info({
                    bucket: bucket.name,
                    old: version,
                    current: bucket.version
                }, 'putBucket: bucket updated');

                next(null, null, false);
            });
        });
    }, callback);
}


/*
 * Reindex all of the objects within a bucket.
 */
function reindex(opts, callback) {
    var bucket = opts.bucket;
    var log = opts.log;
    var moray = opts.moray;

    var processed = 0;
    var count = 100;

    var options = {
        noBucketCache: true
    };

    repeat(function _index(next) {
        moray.reindexObjects(bucket.name, count, options, function (err, res) {
            if (err) {
                next(err, null, false);
                return;
            }

            if (res.processed > 0) {
                log.info({
                    bucket: bucket.name,
                    processed: processed,
                    cur: res.processed
                }, 'reindex: records reindexed');

                processed += res.processed;
                next(null, null, true);
                return;
            }

            next(null, null, false);
        });
    }, function (afterErr) {
        if (afterErr) {
            callback(afterErr);
            return;
        }

        if (processed === 0) {
            log.info({
                bucket: bucket.name
            }, 'reindex: records already reindexed');
        } else {
            log.info({
                bucket: bucket.name
            }, 'reindex: all records reindexed');
        }

        callback();
    });
}


/*
 * Find all old records in a bucket and upgrade them to the latest version of
 * the object.
 */
function updateRecords(opts, callback) {
    var bucket = opts.bucket;
    var app = opts.app;
    var log = opts.log;
    var moray = opts.moray;
    var extra = opts.extra;

    var processed = 0;

    repeat(function _processBatch(next) {
        listObjs({
            app: app,
            extra: extra,
            filter: util.format('(|(!(_v=*))(_v<=%d))', bucket.version - 1),
            log: log,
            bucket: bucket,
            model: bucket.constructor,
            moray: moray,
            noBucketCache: true
        }, function (listErr, recs) {
            if (listErr) {
                next(listErr, null, false);
                return;
            }

            if (recs.length === 0) {
                // No more unmigrated records
                next(null, null, false);
                return;
            }

            var batch = [];
            recs.forEach(function (r) {
                var b = r.batch({ migration: true });
                if (Array.isArray(b)) {
                    batch = batch.concat(b);
                } else {
                    batch.push(b);
                }
            });

            log.debug({
                batch: batch,
                bucket: bucket.name
            }, 'updateRecords: batch');

            moray.batch(batch, function (batchErr) {
                if (batchErr) {
                    if (VError.findCauseByName(batchErr, 'EtagConflictError')) {
                        // One of the batch objects has been updated from
                        // under us: try it again next time
                        next(batchErr, null, true);
                        return;
                    }

                    next(batchErr, null, false);
                    return;
                }

                processed += batch.length;
                log.info({
                    bucket: bucket.name,
                    processed: processed,
                    cur: batch.length
                }, 'updateRecords: records migrated');

                // Migration succeeded - keep going
                next(null, null, true);
            });
        });
    }, function (afterErr) {
        if (afterErr) {
            callback(afterErr);
            return;
        }

        if (processed === 0) {
            log.info({
                bucket: bucket.name
            }, 'updateRecords: records already migrated');
        } else {
            log.info({
                bucket: bucket.name,
                version: bucket.version,
                processed: processed
            }, 'updateRecords: all records migrated');
        }

        callback();
    });
}


/*
 * Ensures a bucket has been created, and takes care of updating it and its
 * contents if it already exists.
 *
 * @param opts {Object]:
 * - `moray`: {Moray Client}
 * - `bucket` {Object}: bucket definition
 * - `log` {Bunyan logger}
 * - `extra` {Object} (optional): extra parameters to pass to constructor
 * @param callback {Function} `function (err)`
 */
function initializeBucket(opts, callback) {
    assert.object(opts, 'opts');
    assert.func(callback, 'callback');
    assert.object(opts.app, 'opts.app');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.moray, 'opts.moray');
    assert.optionalObject(opts.extra, 'opts.extra');

    assert.object(opts.bucket, 'opts.bucket');
    assert.string(opts.bucket.name, 'opts.bucket.name');
    assert.number(opts.bucket.version, 'opts.bucket.version');
    assert.func(opts.bucket.constructor, 'opts.bucket.constructor');
    assert.number(opts.bucket.morayVersion, 'opts.bucket.morayVersion');
    assert.object(opts.bucket.schema, 'opts.bucket.schema');
    assert.object(opts.bucket.schema.index, 'opts.bucket.schema.index');
    assert.object(opts.bucket.schema.index._v, 'opts.bucket.schema.index._v');

    var bucket = opts.bucket;
    var log = opts.log;

    log.info('begin migration for bucket %s', bucket.name);
    vasync.pipeline({
        funcs: [ putBucket, reindex, updateRecords ],
        arg: {
            app: opts.app,
            bucket: bucket,
            extra: opts.extra || {},
            log: log,
            moray: opts.moray
        }
    }, function (err, res) {
        if (err) {
            callback(err);
            return;
        }

        log.trace({ bucket: bucket.name, res: res }, 'migration complete');
        log.info('end migration for bucket %s', bucket.name);
        callback();
    });
}


// --- Exports


/*
 * Creates a new moray client, setting up reconnection logic in the
 * process
 *
 * @param config {Object}
 * @param parentLog {Bunyan Logger Object}
 * @param callback {Function} `function (err, client)`
 */
function createClient(config, parentLog, callback) {
    var conf = {
        connectTimeout: 1000,
        host: config.host,
        noCache: true,
        port: config.port,
        reconnect: true,
        retry: {
            maxTimeout: 6000,
            minTimeout: 100
        }
    };

    conf.log = parentLog.child({
        component: 'moray',
        level: config.logLevel || parentLog.level()
    });
    conf.log.debug(conf, 'Creating moray client');
    waitForConnect(mod_moray.createClient(conf), callback);
}


/**
 * Wait for a Moray client to issue a 'connect' or 'error' event. Log a message
 * every time a connection attempt is made.
 */
function waitForConnect(client, callback) {
    function onMorayConnect() {
        client.removeListener('error', onMorayError);
        client.log.info('moray: connected');
        client.on('error', function (err) {
            // not much more to do because the moray client should take
            // care of reconnecting, etc.
            client.log.error(err, 'moray client error');
        });
        callback(null, client);
    }

    function onMorayError(err) {
        client.removeListener('connect', onMorayConnect);
        client.log.error(err, 'moray: connection failed');
        callback(err);
    }

    function onMorayConnectAttempt(number, delay) {
        var level;
        if (number === 0) {
            level = 'info';
        } else if (number < 5) {
            level = 'warn';
        } else {
            level = 'error';
        }
        client.log[level]({
                attempt: number,
                delay: delay
        }, 'moray: connection attempted');
    }

    client.once('connect', onMorayConnect);
    client.once('error', onMorayError);
    client.on('connectAttempt', onMorayConnectAttempt); // this we always use
}


/*
 * Lists objects in moray
 *
 * @param opts {Object}
 * - `bucket` {Bucket schema object}
 * - `filter` {String}
 * - `limit` {Integer}
 * - `log` {Bunyan Logger}
 * - `offset` {Integer}
 * - `moray` {MorayClient}
 * - `sort` {Object} (optional)
 * - `model` {Object} (optional)
 * - `noBucketCache` {Boolean} (optional)
 * - `extra` {Object} (optional) extra params to pass to constructor
 * @param callback {Function} `function (err, netObj)`
 */
function listObjs(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.app, 'opts.app');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.moray, 'opts.moray');
    assert.object(opts.bucket, 'opts.bucket');
    assert.string(opts.filter, 'opts.filter');
    assert.func(callback, 'callback');

    assert.optionalFunc(opts.model, 'opts.model');
    assert.optionalObject(opts.sort, 'opts.sort');
    assert.optionalObject(opts.extra, 'opts.extra');
    assert.optionalNumber(opts.limit, 'opts.limit');
    assert.optionalNumber(opts.offset, 'opts.offset');
    assert.optionalBool(opts.noBucketCache, 'opts.noBucketCache');

    var results = [];
    var listOpts = {};

    if (opts.sort) {
        listOpts.sort = opts.sort;
    }

    if (opts.limit) {
        listOpts.limit = opts.limit;
    }

    if (opts.offset) {
        listOpts.offset = opts.offset;
    }

    if (opts.noBucketCache) {
        listOpts.noBucketCache = true;
    }

    opts.log.debug({ filter: opts.filter }, 'listObjs: Querying Moray');

    var req = opts.moray.findObjects(opts.bucket.name, opts.filter, listOpts);

    req.on('error', callback);

    req.on('record', function _onListRec(rec) {
        opts.log.trace({ record: rec }, 'record from Moray');
        if (opts.extra) {
            Object.keys(opts.extra).forEach(function (k) {
                rec.value[k] = opts.extra[k];
            });
        }
        results.push(rec);
    });

    req.on('end', function _endList() {
        if (opts.model) {
            async.map(results, function (rec, cb) {
                try {
                    cb(null, new opts.model(rec, opts.app));
                } catch (e) {
                    cb(e);
                }
            }, callback);
        } else {
            callback(null, results);
        }
    });
}


/*
 * Migrates records in the buckets for each of the provided models.
 *
 * @param opts {Object}:
 * - `moray` {Moray Client}
 * - `log` {Bunyan logger}
 * - `buckets` {Array}: array of bucket objects for each model
 *  e.g. [ { constructor: mod_rule.Rule, name: 'fwapi_rules', ... } ]
 * - `extra` {Object} (optional): extra params to pass to constructors
 * @param callback {Function} `function (err)`
 */
function initializeBuckets(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.app, 'opts.app');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.moray, 'opts.moray');
    assert.arrayOfObject(opts.buckets, 'opts.buckets');
    assert.optionalObject(opts.extra, 'opts.extra');
    assert.func(callback, 'callback');

    var log = opts.log;
    var buckets = opts.buckets;

    // If any migrations require a newer Moray, wait until it's been upgraded.
    repeat(function _checkVersion(next) {
        opts.moray.versionInternal({ log: log }, function (err, v) {
            if (err) {
                log.error(err, 'Error getting version, will check again');
                next(null, null, true);
                return;
            }

            var retry = false;

            buckets.forEach(function (bucket) {
                if (bucket.morayVersion > v) {
                    log.error('Moray is at version %d but bucket ' +
                        '"%s" requires Moray version %d', v, bucket.name,
                        bucket.morayVersion);
                    retry = true;
                }
            });

            if (retry) {
                log.error('Will check for a newer Moray in 10 seconds');
                setTimeout(next, 10000, null, null, true);
            } else {
                next(null, null, false);
            }
        });
    }, function (_) {
        vasync.forEachPipeline({
            func: function migrateOne(bucket, cb) {
                initializeBucket({
                    app: opts.app,
                    log: log,
                    extra: opts.extra,
                    moray: opts.moray,
                    bucket: bucket
                }, cb);
            },
            inputs: buckets
        }, function (err, res) {
            log.debug({ err: err, res: res }, 'migration results');
            callback(err);
        });
    });
}


module.exports = {
    create: createClient,
    initialize: initializeBuckets,
    listObjs: listObjs,
    waitForConnect: waitForConnect
};
