#!/usr/bin/env node
/**
 * Copyright 2013 Yegor Bugayenko, 2015 Dmitriy Mozgovoy, Playkot
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var async = require('async'),
    util = require('util'),
    utils = require('../lib/utils'),
    lineReader = require('line-reader'),
    sleep = require('sleep');

var argv = utils.config({
    demand: ['table'],
    optional: ['rate', 'key', 'secret', 'region', 'report', 'skip', 'batch'],
    default: {
        skip: 0
    },
    usage: 'Restores Dynamo DB table from JSON file\n' +
        'Usage: dynamo-restore --table my-table [--rate 100] [--skip 0] [--region us-east-1] [--key AK...AA] [--secret 7a...IG] [--batch] filename'
});

var BATCH_MAX_SIZE = 25;
var WRITE_UNIT_SIZE = 1024;

var dynamo = utils.dynamo(argv);
dynamo.describeTable(
    {
        TableName: argv.table
    },
    function (err, data) {
        if (err != null) {
            throw err;
        }
        if (data == null) {
            throw 'Table ' + argv.table + ' not found in DynamoDB';
        }
        var quota = data.Table.ProvisionedThroughput.WriteCapacityUnits,
            portion = [],
            units = 0,
            skipped = 0,
            done = 0,
            writtenRows = 0;

        if (!argv.filename) throw new Error('Last argument is filename, it`s required');

        console.log(util.format(
            '[%s]: Restoring table %s from file %s. Write capacity quota: %d',
            new Date(),
            argv.table,
            argv.filename,
            quota
        ));

        if (argv.skip) console.log('Skipping ', argv.skip, 'rows...');

        lineReader.eachLine(argv.filename, function (line, last, callback) {
                if (skipped < argv.skip) {
                    skipped++;
                    if (skipped % quota == 0) done++;
                    return callback();
                }

                var object = {};
                line = JSON.parse(line);
                for (var i in line) {
                    object[i] = line[i];
                    var size;
                    if (object[i].B) {
                        object[i].B = new Buffer(object[i].B);
                        size = Math.ceil(object[i].B.length/WRITE_UNIT_SIZE);
                    } else {
                        size = Math.ceil(Buffer.byteLength(line[i], 'utf-8')/WRITE_UNIT_SIZE);
                    }

                    units += size;
                    if (argv.batch && size >= 400) {
                        return callback(new Error('UNable to use batch processing for items bigger than 400KB'));
                    }
                }

                portion.push(object);
                if (units < quota) return callback();

                var start = new Date().getTime();
                var onSave = function(err) {
                    if (err) throw err;

                    done++;
                    writtenRows += portion.length;

                    console.log(util.format(
                        '[%s]: Portion #%d is done. Items written: %d. Items written overall: %d',
                        new Date(),
                        done,
                        portion.length,
                        writtenRows
                    ));

                    units = 0;
                    portion = [];

                    var sleepTime = (1000 - (new Date().getTime() - start));
                    sleep.usleep(Math.max(sleepTime * 1100, 0));
                    callback();
                };

                if (argv.batch) {
                    batchSaveItems(dynamo, argv.table, portion, onSave);
                } else {
                    saveItems(dynamo, argv.table, portion, onSave);
                }
            }
        );
    }
);

function saveItems(dynamo, table, items, callback) {
    async.each(items, function(item, callback) {
        dynamo.putItem(
            {
                TableName: table,
                Item: item
            },
            callback
        );
    }, callback);
}

function batchSaveItems(dynamo, table, items, callback) {
    var portions = [];
    for (var i=0; i<=items.length/BATCH_MAX_SIZE; i++) {
        portions.push(items.slice(i * BATCH_MAX_SIZE, (i+1) * BATCH_MAX_SIZE));
    }

    async.each(portions, function(portion, callback) {
        if (!portion.length) return callback();
        makeBatchWriteRequest(dynamo, table, portion, callback);
    }, callback);
}

function makeBatchWriteRequest(dynamo, table, items, callback) {
    if (!items.length) return callback(new Error('Items arg should be an array'));
    if (items.length > BATCH_MAX_SIZE) return callback(new Error('Items arg is larger than 25 items (' + items.length + ')'));

    var params = {
        RequestItems: {}
    };
    params.RequestItems[table] = [];

    for (var i in items) {
        params.RequestItems[table].push({
            PutRequest: {
                Item: items[i]
            }
        });
    }

    dynamo.batchWriteItem(params, function(err, result) {
        if (err) return callback(err);
        if (!result.UnprocessedItems[table]) return callback();

        dynamo.batchWriteItem({
            RequestItems: result.UnprocessedItems
        }, callback);
    });
}
