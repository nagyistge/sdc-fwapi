/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * Mock objects for FWAPI unit tests
 */

'use strict';

var EventEmitter = require('events').EventEmitter;
var mod_filter = require('moray-filter');
var util = require('util');
var VError = require('verror').VError;



// --- Globals



var LOG;
var UFDS_RULES = {};



// --- sdc-clients: UFDS



function fakeUFDSclient() {
    var self = this;
    EventEmitter.call(this);
    process.nextTick(function () {
        self.emit('connect');
    });
}

util.inherits(fakeUFDSclient, EventEmitter);


function ruleUUIDfromDN(dn) {
    /* JSSTYLED */
    var ruleUUID = dn.match(/uuid=([^,]+),/);
    return ruleUUID ? ruleUUID[1] : null;
}


fakeUFDSclient.prototype.add = function (dn, raw, callback) {
    var ruleUUID = ruleUUIDfromDN(dn);
    if (ruleUUID) {
        UFDS_RULES[ruleUUID] = raw;
    }

    return callback(null);
};


fakeUFDSclient.prototype.del = function (dn, callback) {
    var ruleUUID = ruleUUIDfromDN(dn);
    if (ruleUUID) {
        if (!UFDS_RULES.hasOwnProperty(ruleUUID)) {
            return callback(
                new VError('Rule "%s" does not exist in UFDS', ruleUUID));
        }
        delete UFDS_RULES[ruleUUID];
    }

    return callback(null);
};


fakeUFDSclient.prototype.close = function (callback) {
    return callback();
};


fakeUFDSclient.prototype.modify = function (dn, change, callback) {
    var ruleUUID = ruleUUIDfromDN(dn);
    if (ruleUUID) {
        UFDS_RULES[ruleUUID] = change.modification;
    }

    return callback(null);
};


fakeUFDSclient.prototype.search = function (dn, opts, callback) {
    var rule;

    if (opts.scope === 'base') {
        var ruleUUID = ruleUUIDfromDN(dn);
        rule = UFDS_RULES[ruleUUID];
        if (!ruleUUID || !rule) {
            callback(null, []);
            return;
        }
        callback(null, [ rule ]);
        return;
    } else if (opts.scope === 'sub') {
        var results = [];
        for (rule in UFDS_RULES) {
            if (mod_filter.parse(opts.filter).matches(UFDS_RULES[rule])) {
                results.push(UFDS_RULES[rule]);
            }
        }
        callback(null, results);
        return;
    }

    callback(new Error('xxxx'));
};



// --- sdc-clients: VMAPI



function fakeVMAPIclient() {

}



module.exports = {
    // -- mocks

    'sdc-clients': {
        VMAPI: fakeVMAPIclient
    },

    ufds: fakeUFDSclient,

    // -- mock data
    set _LOGGER(val) {
        LOG = val;
    }
};
