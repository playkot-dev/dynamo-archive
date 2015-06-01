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

var utils = require('../lib/utils'),
    lineReader = require('line-reader'),
    sleep = require('sleep');

var argv = utils.config({
    demand: ['table'],
    optional: ['rate', 'key', 'secret', 'region'],
    usage: 'Restores Dynamo DB table from JSON file\n' +
        'Usage: dynamo-restore --table my-table [--rate 100] [--region us-east-1] [--key AK...AA] [--secret 7a...IG] filename'
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
            msecPerItem = Math.round(1000 / quota / ((argv.rate || 100) / 100)),
            reportPeriod = argv.reportPeriod || 1000,
            filename = argv._[0],
            start = Date.now(),
            done = 0;

        if (!filename) throw new Error('Last argument is filename, it`s required');

        lineReader.eachLine(filename, function (line, last, callback) {
                var object = {};
                line = JSON.parse(line);
                for (var i in line) {
                    object[i] = line[i];
                    if (object[i].B) object[i].B = new Buffer(object[i].B);
                }

                dynamo.putItem(
                    {
                        TableName: argv.table,
                        Item: object
                    },
                    function (err) {
                        object = undefined;
                        if (err) {
                            console.log(err, err.stack);
                            throw err;
                        }

                        ++done;
                        if (done % reportPeriod == 0) console.log('Done:', done);
                        var expected = start + msecPerItem * done;
                        if (expected > Date.now()) {
                            sleep.usleep((expected - Date.now()) * 1000);
                        }
                        callback();
                    }
                );
            }
        );
    }
);
