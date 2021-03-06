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

var EventEmitter = require('events').EventEmitter;
var mockMoray = require('../lib/mock-moray');
var mod_uuid = require('node-uuid');
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
    if (opts.scope === 'base') {
        var ruleUUID = ruleUUIDfromDN(dn);
        var rule = UFDS_RULES[ruleUUIDfromDN(dn)];
        if (!ruleUUID || !rule) {
            return callback(null, []);
        }
        return callback(null, [ rule ]);
    }

    return callback(new Error('xxxx'));
};



// --- sdc-clients: VMAPI



function fakeVMAPIclient() {

}



module.exports = {
    // -- mocks

    moray: mockMoray,

    'sdc-clients': {
        VMAPI: fakeVMAPIclient
    },

    ufds: fakeUFDSclient,

    // -- mock data
    set _LOGGER(val) {
        LOG = val;
    },

    get _BUCKETS() {
        return mockMoray._buckets;
    }
};
