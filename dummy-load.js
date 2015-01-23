/* Catalog core - data load script

   Copyright 2014 Commons Machinery http://commonsmachinery.se/

   Distributed under an AGPL_v3 license, please see LICENSE in the top dir.
*/

'use strict';

var debug = require('debug')('catalog:scripts:dummy-load');
var _ = require('underscore');
var Promise = require('bluebird');

// Script libs
var fs = require('fs');
var ldj = require('ldjson-stream');

var argv = require('yargs')
    .boolean('verbose').default('verbose', false)
    .string('user')
    .boolean('private').default('private', false)
    .string('format').default('format', 'datapackage')
    .string('ownerOrg').default('ownerOrg', undefined)
    .boolean('keepgoing').default('keepgoing', false)
    .demand('_')
    .argv;


// Return the first property with a matching name
var getProperty = function(propertyName, annotations) {
    for (var i = 0; i < annotations.length; i++) {
        var a = annotations[i];
        if (a.propertyName === propertyName) {
            return a;
        }
    }

    return null;
};

var getIdentifierLink = function(annotations) {
    var a = getProperty('identifier', annotations);
    return a && a.identifierLink;
};

var getLocatorLink = function(annotations) {
    var a = getProperty('locator', annotations);
    return a && a.locatorLink;
};

var saveDocument = function(doc) {
    return new Promise(function(resolve, reject) {
        doc.save(function(err, savedDoc, numberAffected) {
            if (err) {
                debug('saving work failed: %j', err);
                reject(err);
                return;
            }

            resolve(savedDoc);
        });
    });
};

var processDataPackage = function(inputStream, context, owner, priv, verbose, done) {
    var stream = inputStream.pipe(ldj.parse());
    var errorFileName = 'errors_dummy_load_' + (new Date()).toISOString() + '.json';
    var outputFileName = 'output_dummy_load' + (new Date()).toISOString() + '.json';
    var count = 0;

    var logError = function(obj) {
        fs.appendFile(errorFileName, JSON.stringify(obj) + '\n');
    };

    // Process work records
    stream.on('data', function(obj) {
        var workURI = getIdentifierLink(obj.annotations) || getLocatorLink(obj.annotations);

        ++count;

        if (!workURI) {
            // There must be a work URI
            console.error('%s: error: no identifier or locator link (json written to error file)', count);
            logError(obj);
            return;
        }

        // Don't get more events while we're processing this one
        stream.pause();

        fs.appendFile(outputFileName, JSON.stringify(obj) + '\n');
        console.log(workURI + ' ' + 'ok');

        stream.resume();
    });

    stream.on('end', function() {
        done(null);
    });
};

var main = function() {
    var priv;
    var processPackage;
    var userId, ownerOrgId;

    priv = argv.private;

    if (argv.format && argv.format === 'datapackage') {
        processPackage = processDataPackage;
    } else {
        throw new Error('Unknown format: ' + argv.format);
    }

    Promise.resolve(true)
        .then(function() {
            var context;
            var owner;

            context = { userId: null };

            processPackage(process.stdin, context, owner, priv, argv.verbose, function(err) {
                // wait 1s to make sure the process chain catches up
                setTimeout(function() {
                    if (err) {
                        process.exit(1);
                    } else {
                        process.exit(0);
                    }
                }, 1000);
            });
        })
        .catch(function(err) {
            console.error('error starting core backend: %s', err);
            process.exit(1);
        });
};

if (require.main === module) {
    main();
}
