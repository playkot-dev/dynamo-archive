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
    utils = require('../lib/utils'),
    lineReader = require('line-reader'),
    sleep = require('sleep');

var argv = utils.config({
    demand: ['table'],
    optional: ['rate', 'key', 'secret', 'region', 'report', 'skip'],
    default: {
        skip: 0
    },
    usage: 'Restores Dynamo DB table from JSON file\n' +
        'Usage: dynamo-restore --table my-table [--rate 100] [--skip 0] [--region us-east-1] [--key AK...AA] [--secret 7a...IG] filename'
});

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
            skipped = 0,
            done = 0;

        if (!argv.filename) throw new Error('Last argument is filename, it`s required');

        console.log(new Date(), 'Restoring table', argv.table, 'from file', argv.filename, 'quota:', quota);
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
                    if (object[i].B) object[i].B = new Buffer(object[i].B);
                }

                portion.push(object);
                if (portion.length < quota) return callback();

                saveItems(dynamo, argv.table, portion, function(err) {
                    if (err) throw err;

                    done++;
                    console.log(new Date(), 'Portion #', done, 'sent. Rows count:', done * quota);
                    portion = [];
                    callback();
                });
            }
        );
    }
);

function saveItems(dynamo, table, items, callback) {
    var start = new Date().getTime();
    async.each(items, function(item, callback) {
        dynamo.putItem(
            {
                TableName: table,
                Item: item
            },
            callback
        );
    }, function(err) {
        if (err) return callback(err);

        var sleepTime = (1000 - (new Date().getTime() - start));
        sleep.usleep(Math.max(sleepTime * 1100, 0));
        callback();
    });
}
