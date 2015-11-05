var AWS = require('aws-sdk');
var prompt = require('prompt');
var fs = require('fs');
var async = require('async');
var chalk = require('chalk');
var commandLineArgs = require('command-line-args');
var cli = commandLineArgs([
    {
        name: 'profile',
        description: 'S3 credentials profile to use',
        alias: 'p',
        type: String
    },
    {
        name: 'bucket',
        description: 'Bucket to access',
        alias: 'b',
        type: String
    }
]);
var options = cli.parse();
if (!options.profile) {
    console.log(chalk.green('Usage: '));
    console.log(cli.getUsage());
    return;
}
var credentials = new AWS.SharedIniFileCredentials({
    profile: options.profile
});
AWS.config.credentials = credentials;
var s3 = new AWS.S3();

var bucket = options.bucket;

var confirmExit = function(loop, next) {
    console.log('Are you sure you want to exit? [y/n]');
    prompt.start();
    prompt.message = "?".green;

    prompt.get({
        properties: {
            'command': {
                required: true,
                type: 'string',
                enum: ['y', 'n']
            }
        }
    }, function(err, result) {
        if (result.command == 'y') {
            return next('Exit');
        }
        loop();
    });
};

var getFilesCurrentDirectory = function(folder, next) {
    var allFiles = [];
    var nextMarker;
    var commonPrefixes = {};
    var returnVars = {};
    async.forever(function(loop) {
        var filter = {
            Bucket: bucket,
            Delimiter: '/',
            MaxKeys: 1000
        };
        if (nextMarker) {
            filter.Marker = nextMarker;
        }
        if (folder) {
            filter.Prefix = folder;
        }
        s3.listObjects(filter, function(err, data) {
            if (err) {
                return loop(err);
            }
            async.eachSeries(data.CommonPrefixes, function(prefix, callback) {
                prefix = prefix.Prefix;
                commonPrefixes[prefix] = 1;
                if (prefix == '/') {
                    getFilesCurrentDirectory(prefix, function(useless, returnVars) {
                        allFiles = returnVars.allFiles;
                        for (var i = 0; i < returnVars.commonPrefixes.length; i++) {
                            commonPrefixes[returnVars.commonPrefixes[i]] = 1;
                        }
                        callback();
                    });
                } else {
                    callback();
                }
            }, function() {
                if (allFiles.length = 0) {
                    allFiles = data.Contents;
                } else {
                    for (var i = 0; i < data.Contents.length; i++) {
                        allFiles.push(data.Contents[i]);
                    }
                }
                if (data.IsTruncated) {
                    if (data.NextMarker) {
                        nextMarker = data.NextMarker;
                    } else {
                        nextMarker = data.Contents[data.Contents.length - 1];
                    }
                    loop();
                } else {
                    returnVars.allFiles = allFiles;
                    returnVars.commonPrefixes = Object.keys(commonPrefixes);
                    next(null, returnVars);
                }
            });
        });
    }, function(err) {
        if (err) {
            return next(err);
        }
    });
};

var listFiles = function(allFiles, folders, currentFolder, next) {
    console.log('Files under ' + currentFolder + ':');
    for (var i = 0; i < folders.length; i++) {
        console.log(chalk.yellow(folders[i].substring(currentFolder.length)));
    }
    var limit = 25;
    var counter = 0;
    async.eachSeries(allFiles, function(file, callback) {
        if (counter < limit) {
            console.log(file.Key.substring(currentFolder.length));
            counter++;
            callback();
        } else {
            prompt.start();
            prompt.get({
                properties: {
                    'Type \'it\' for more': {
                        enum: ['it', 'quit', 'exit'],
                        required: true
                    }
                }
            }, function(err, result) {
                if (result && result[Object.keys(result)[0]] == 'it') {
                    limit += 25;
                    callback();
                } else {
                    next();
                }
            });
        }
    }, function(err) {
        if (err) {
            return next(err);
        }
        next();
    });
};

async.waterfall([
    function(next) {
        if (bucket) {
            return next();
        }
        s3.listBuckets(function(err, data) {
            if (err) {
                return next(err);
            }
            console.log(chalk.green('Buckets: '));
            var buckets = data.Buckets;
            var bucketNames = [];
            for (var i = 0; i < buckets.length; i++) {
                var inBucket = buckets[i];
                bucketNames.push(inBucket.Name);
                console.log(chalk.green(inBucket.Name + '\tcreated: ' + inBucket.CreationDate));
            }
            prompt.start();
            prompt.get({
                properties: {
                    'bucket': {
                        required: true,
                        enum: bucketNames
                    }
                }
            }, function(err, result) {
                if (!result) {
                    return next('Bucket is required.');
                }
                bucket = result.bucket;
                next();
            });
        });
    },
    function(next) {
        getFilesCurrentDirectory(null, next);
    },
    function(returnVars, next) {
        var allFiles = returnVars.allFiles;
        var commonPrefixes = returnVars.commonPrefixes;
        listFiles(allFiles, commonPrefixes, '', function() {
            return next(null, allFiles, commonPrefixes);
        });
    },
    function(allFiles, commonPrefixes, next) {
        var currentFolder = '';
        var lastCommonPrefixes = commonPrefixes;
        var lastAllFiles = allFiles;
        console.log('Type help for help.');
        async.forever(function(loop) {
            prompt.start();
            prompt.message = "?".green;

            prompt.get({
                properties: {
                    'command': {
                        required: true,
                        type: 'string',
                        pattern: '^\\s*((help)|(cd)|(get)|(search)|(ls))(\\s+\\S+)?\\s*$'
                    }
                }
            }, function(err, result) {
                if (!result) {
                    return confirmExit(loop, next);
                }
                var command = result.command.trim();
                if (new RegExp('^\\s*help').test(command)) {
                    console.log(chalk.green('cd: '));
                    console.log('\tEnter a directory. eg: cd prefix');
                    console.log(chalk.green('get: '));
                    console.log('\tDownload a file with the given key.');
                    console.log('\tThe key should be a relative path under current directory.');
                    console.log(chalk.green('search: '));
                    console.log('\tSearch for a file with the given regular expression. eg: search ^\\w\\.csv$');
                    loop();

                } else if (new RegExp('^cd').test(command)) {
                    var cd = new RegExp('^cd').exec(command);
                    command = command.substring(cd.length + 1).trim();
                    if (command.length == 0) {
                        console.log(chalk.red('Please enter a folder after \'cd\'.'));
                        return loop();
                    }
                    var found = false;
                    if (!new RegExp('/$').test(command)) {
                        command += '/';
                    }
                    if (command == './' || command == '/') {
                        command = '';
                        found = true;
                    }

                    if (command == '../') {
                        var lastFolder = new RegExp('\\w+/$').exec(currentFolder);
                        if (lastFolder) {
                            currentFolder = currentFolder.substring(0, currentFolder.length - lastFolder[0].length);
                            if (currentFolder == '/') {
                                currentFolder = '';
                            }
                        } else {
                            console.log(chalk.red('Cannot go beyond root.'));
                            return loop();
                        }
                        found = true;
                    }
                    for (var i = 0; i < lastCommonPrefixes.length; i++) {
                        if (lastCommonPrefixes[i] == command || lastCommonPrefixes[i] == (currentFolder + command)) {
                            found = true;
                            currentFolder += command;
                        }
                    }
                    if (!found) {
                        console.log(chalk.red('Please enter a valid folder.'));
                        return loop();
                    }
                    getFilesCurrentDirectory(currentFolder, function(useless, returnVars) {
                        var allFiles = returnVars.allFiles;
                        var commonPrefixes = returnVars.commonPrefixes;
                        lastCommonPrefixes = commonPrefixes;
                        lastAllFiles = allFiles;
                        listFiles(allFiles, commonPrefixes, currentFolder, loop);
                    });
                } else if (new RegExp('^ls').test(command)) {
                    getFilesCurrentDirectory(currentFolder, function(useless, returnVars) {
                        var allFiles = returnVars.allFiles;
                        var commonPrefixes = returnVars.commonPrefixes;
                        listFiles(allFiles, commonPrefixes, currentFolder, loop);
                    });
                } else if (new RegExp('^get').test(command)) {
                    var get = new RegExp('^get').exec(command);
                    command = command.substring(get[0].length + 1).trim();
                    async.each(lastAllFiles, function(file, callback) {
                        if (!new RegExp('\\.*' + command + '\\.*').test(file.Key)) {
                            return callback();
                        }
                        var basename = file.Key.substring(currentFolder.length);
                        var stream = fs.createWriteStream(basename, {
                            flags: 'w',
                            encoding: null,
                            mode: '0666'
                        });
                        s3.getObject({
                            Bucket: bucket,
                            Key: file.Key
                        })
                            .on('httpData', function(data) {
                                stream.write(data);
                            })
                            .on('complete', function() {
                                console.log(chalk.green('Downloaded ' + basename))
                                stream.end();
                                callback();
                            }).send();
                    }, function() {
                        loop();
                    });
                }
            });
        }, function(err) {
            next(err);
        });
    },

], function(err) {
    if (err)
        console.log(chalk.red(err));
});



