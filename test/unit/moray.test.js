/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

/*
 * Moray bucket setup tests
 */

'use strict';

var h = require('./helpers');
var mod_log = require('../lib/log');
var mod_moray = require('../../lib/moray');
var mod_rule = require('../../lib/rule');
var test = require('tape');

// --- Globals

var MORAY;
var MUST_STOP = false;

var LOG = mod_log.selectUnitLogger();


// --- Helpers

function startupFailure(t, mockFailures) {
    mod_moray.waitForConnect(MORAY.clone(), function (wErr, moray) {
        if (h.ifErr(t, wErr, 'waitForConnect() error')) {
            t.end();
            return;
        }

        moray.setMockErrors(mockFailures);

        h.createClientAndServer({
            log: LOG,
            moray: moray
        }, function (cErr) {
            t.ok(cErr, 'FWAPI startup error');

            h.stopServer(function (sErr) {
                t.ifError(sErr, 'server stop');
                t.end();
            });
        });
    });
}


function startupSuccess(t) {
    mod_moray.waitForConnect(MORAY.clone(), function (wErr, moray) {
        if (h.ifErr(t, wErr, 'waitForConnect() error')) {
            t.end();
            return;
        }

        h.createClientAndServer({
            log: LOG,
            moray: moray
        }, function (cErr, res) {
            t.ok(res, 'client');
            if (h.ifErr(t, cErr, 'FWAPI startup error')) {
                t.end();
                return;
            }

            h.stopServer(function (sErr) {
                t.ifError(sErr, 'server stop');
                t.end();
            });
        });
    });
}


// --- Setup

if (!h.MULTI_SUITE_RUN) {
    h.MULTI_SUITE_RUN = true;
    MUST_STOP = true;
}


// --- Tests

test('putBucket() failures and rollbacks', function (t) {
    var tError = new Error('Query timed out');
    tError.name = 'QueryTimeoutError';

    t.plan(8);

    t.test('Start Moray server', function (t2) {
        h.setupMoray(LOG, function (err, moray) {
            t2.ifErr(err, 'Moray setup error');
            t2.ok(moray, 'moray');

            MORAY = moray;
            t2.end();
        });
    });

    t.test('Fail to start FWAPI (getBucket() failures)', function (t2) {
        startupFailure(t2, {
            getBucket: [ tError, tError, tError, tError, tError, tError ]
        });
    });

    t.test('Fail to start FWAPI (createBucket() failures)', function (t2) {
        startupFailure(t2, {
            createBucket: [ tError, tError, tError, tError, tError, tError ]
        });
    });

    t.test('Start FWAPI (creates bucket)', startupSuccess);

    t.test('Start FWAPI (no bucket changes)', startupSuccess);

    t.test('Fail to start FWAPI (updateBucket() failures)', function (t2) {
        // Roll version forward and restart
        mod_rule.BUCKET.version += 1;
        startupFailure(t2, {
            updateBucket: [ tError, tError, tError, tError, tError, tError ]
        });
    });

    t.test('Start FWAPI (updates bucket)', function (t2) {
        startupSuccess(t2);
    });

    t.test('Start FWAPI (rollback)', function (t2) {
        // Roll back version and restart
        mod_rule.BUCKET.version -= 1;
        startupSuccess(t2);
    });
});


// --- Teardown

test('Close Moray client', function (t) {
    MORAY.close();
    t.end();
});


if (MUST_STOP) {
    test('Stop PG', function (t) {
        h.stopPG();
        t.end();
    });
}
