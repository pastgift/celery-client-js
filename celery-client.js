'use strict';

var uuid = require('uuid');

var nope = function() {};

var canonicalizeTaskOptions = function(taskOptions) {
  taskOptions = taskOptions || {};

  // Canonicalize header
  taskOptions.id = (taskOptions.id || uuid.v4()).toString();

  taskOptions.eta = taskOptions.eta
                  ? new Date(taskOptions.eta).toISOString()
                  : null
  taskOptions.expires = taskOptions.expires
                  ? new Date(taskOptions.expires).toISOString()
                  : null

  taskOptions.retries = parseInt(taskOptions.retries || 0);

  taskOptions.timeLimit     = taskOptions.timeLimit     || null;
  taskOptions.softTimeLimit = taskOptions.softTimeLimit || null;

  taskOptions.origin = taskOptions.origin || null;

  // Canonicalize properties
  taskOptions.priority = taskOptions.priority || 0;
  taskOptions.queue = taskOptions.queue || 'celery';

  taskOptions.deliveryMode = 2;
  taskOptions.deliveryTag = uuid.v4();

  return taskOptions;
};

/**
 * Celery Client
 * @param {Object} brokerHandler                           - Broker handler
 * @param {Object} backendHandler                          - Backend handler
 * @param {Object} defaultTaskOptions                      - Default task options
 * @param {Object} [defaultTaskOptions.id=uuid.v4()]       - Task ID
 * @param {Object} [defaultTaskOptions.eta=null]           - Task ETA
 * @param {Object} [defaultTaskOptions.expires=null]       - Task expires
 * @param {Object} [defaultTaskOptions.retries=0]          - Task retry times
 * @param {Object} [defaultTaskOptions.expires=null]       - Task expires
 * @param {Object} [defaultTaskOptions.timeLimit=null]     - Task time limit (in seconds)
 * @param {Object} [defaultTaskOptions.softTimeLimit=null] - Task time limit (soft, in seconds)
 * @param {Object} [defaultTaskOptions.origin=null]        - Task sender name
 * @param {Object} [defaultTaskOptions.priority=null]      - Task priority (0 ~ 255, 0 is the lowest)
 * @param {Object} [defaultTaskOptions.queue='celery']     - Target queue
 */
function Client(brokerHandler, backendHandler, defaultTaskOptions) {
  var self = this;

  self._brokerHandler      = brokerHandler;
  self._backendHandler     = backendHandler;
  self._defaultTaskOptions = canonicalizeTaskOptions(defaultTaskOptions) || {};
};

Client.prototype.putTask = function(task, args, kwargs, taskOptions, callback) {
  var self = this;

  taskOptions = canonicalizeTaskOptions(taskOptions) || {};

  var mergedTaskOptions = JSON.parse(JSON.stringify(self._defaultTaskOptions));
  Object.assign(mergedTaskOptions, taskOptions);

  mergedTaskOptions = mergedTaskOptions;

  self._brokerHandler.putTask(task, args, kwargs, mergedTaskOptions, callback || nope);
};

Client.prototype.getResult = function(taskId, callback) {
  var self = this;

  self._backendHandler.getResult(taskId, callback || nope);
};

Client.prototype.onResult = function(taskId, callback) {
  var self = this;

  self._backendHandler.onResult(taskId, callback || nope);
};

exports.Client = Client;

exports.RedisHandler = require('./handlers/redisHandler');