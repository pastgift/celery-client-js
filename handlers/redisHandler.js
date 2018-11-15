'use strict';

var redis = require('redis');

function once(fn, context) {
    var result;
    return function () {
        if (fn) {
            result = fn.apply(context || this, arguments);
            fn = null;
        }
        return result;
    };
}

function RedisHandler(redisOptions) {
  var self = this;

  self._handler = redis.createClient(redisOptions);

  // Fixed in Celery for saving/publishing task result.
  // See [https://github.com/celery/celery/blob/v4.1.0/celery/backends/base.py#L518]
  self.taskKeyPrefix = 'celery-task-meta-';
};

RedisHandler.prototype.createMessage = function(task, args, kwargs, taskOptions) {
  var self = this;

  // Prepare body
  var embed = {
    'chord'    : null,
    'callbacks': null,
    'errbacks' : null,
    'chain'    : null,
  };
  var message = JSON.stringify([args, kwargs, embed])
  var body = new Buffer(message).toString('base64');

  var message = {
    'body': body,
    'headers': {
      'lang'     : 'py',                          // [Fixed value] ??
      'task'     : task,                          // Task name in Celery
      'id'       : taskOptions.id,                // Task ID
      'root_id'  : taskOptions.id,                // Same to `headers.id`. For chained task tracing
      'parent_id': null,                          // [Fixed value] For chained task tracing
      'group'    : null,                          // [Fixed value] For paellel tasks

      'eta'      : taskOptions.eta,               // ETA (ISO8601, e.g. 2017-08-29T12:47:00.000Z)
      'expires'  : taskOptions.expires,           // Expire time (ISO8601, e.g. 2017-08-29T12:47:00.000Z)
      'retries'  : taskOptions.retries,           // Retry times
      'timelimit': [
        taskOptions.timeLimit,                    // Time limit (in seconds)
        taskOptions.softTimeLimit,                // Soft time limit (raise Exception, in seconds)
      ],
      'origin': taskOptions.origin,               // Senders name
      'extra' : taskOptions.extra,
    },
    'properties': {
      'body_encoding' : 'base64',                 // [Fixed value] Body encoding
      'priority'      : taskOptions.priority,     // Task priority
      'correlation_id': taskOptions.id,           // Same to `headers.id`
      'reply_to'      : null,
      'delivery_info' : {
        'routing_key': taskOptions.queue,         // Queue name
        'exchange'   : null
      },
      'delivery_mode' : taskOptions.deliveryMode, // Fixed value (1: Non-persistent, 2: Persistent)
      'delivery_tag'  : taskOptions.deliveryTag,  // ??
    },
    'content-type'    : 'application/json',       // [Fixed value] Content type
    'content-encoding': 'utf-8',                  // [Fixed value] Content encoding
  };

  return message;
};

RedisHandler.prototype.createResultKey = function(taskId) {
  var self = this;

  var key = self.taskKeyPrefix + taskId;

  return key;
};

RedisHandler.prototype.parseResult = function(rawResult) {
  var self = this;

  var result = null;
  try {
    result = JSON.parse(rawResult);
  } catch(ex) {
    result = rawResult;
  }

  return result;
};

RedisHandler.prototype.putTask = function(name, args, kwargs, taskOptions, callback) {
  var self = this;

  var message = self.createMessage(name, args, kwargs, taskOptions);

  var targetQueue = message.properties.delivery_info.routing_key;
  var taskToSend = JSON.stringify(message);

  var pushFunc = taskOptions.priority > 0 ? 'rpush' : 'lpush';
  self._handler[pushFunc](targetQueue, taskToSend, function(err) {
    if (err) return callback(err);

    var taskId = message.headers.id;

    return callback(err, taskId);
  });
};

RedisHandler.prototype.getResult = function(taskId, callback) {
  var self = this;

  var key = self.createResultKey(taskId);

  self._handler.get(key, function(err, result) {
    if (err) return callback(err);

    result = self.parseResult(result);

    return callback(null, result);
  });
};

RedisHandler.prototype.onResult = function(taskId, callback) {
  var self = this;

  var resultHandler = self._handler.duplicate();

  var resultCallback = once(function(channel, result) {
    resultHandler.unsubscribe();
    resultHandler.quit();

    if (result) {
      result = self.parseResult(result);
    }

    return callback(null, result);
  });

  setTimeout(resultCallback, 3000);
  resultHandler.on('message', resultCallback);

  var key = self.createResultKey(taskId);
  resultHandler.subscribe(key);
};

RedisHandler.prototype.listQueued = function(queue, callback) {
  var self = this;

  self._handler.lrange(queue, 0, -1, function(err, result) {
    if (err) return callback(err);

    for (var i = 0; i < result.length; i++) {
      result[i] = JSON.parse(result[i]);
      result[i].body = JSON.parse(new Buffer(result[i].body, 'base64').toString());
    }

    return callback(null, result);
  });
};

RedisHandler.prototype.listScheduled = function(callback) {
  var self = this;

  self._handler.hgetall('unacked', function(err, taskMap) {
    if (err) return callback(err);

    self._handler.zrange('unacked_index', 0, -1, 'withscores', function(err, tasks) {
      if (err) return callback(err);

      var result = [];
      for (var i = 0; i < tasks.length; i += 2) {
        var taskId = tasks[i];

        var t = JSON.parse(taskMap[taskId])[0];
        t.body = JSON.parse(new Buffer(t.body, 'base64').toString());

        result.push(t);
      }

      return callback(null, result);
    });
  });
};

RedisHandler.prototype.listRecent = function(callback) {
  var self = this;

  self._handler.keys(self.taskKeyPrefix + '*', function(err, metaTaskIds) {
    if (err) return callback(err);

    if (metaTaskIds.length <= 0) {
      return callback(null, []);
    }

    self._handler.mget(metaTaskIds, function(err, result) {
      if (err) return callback(err);

      for (var i = 0; i < result.length; i++) {
        result[i] = JSON.parse(result[i]);
      }

      return callback(null, result);
    });
  });
};

module.exports = RedisHandler;