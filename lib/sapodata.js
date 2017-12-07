'use strict';
// Copyright IBM Corp. 2012,2016. All Rights Reserved.
// Node module: loopback-connector-mongodb
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

/*!
 * Module dependencies
 */
var g = require('strong-globalize')();
var util = require('util');
var _ = require('underscore');
var async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:sapodata');
var Client = require('node-rest-client').Client;
var jsonpath = require('jsonpath');
var moment = require('moment');

/**
 * Initialize the SAPoData connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
	var s = dataSource.settings;

	this.options = s;

	dataSource.connector = new SAPoData(s, dataSource);

	if (callback) {
		if (s.lazyConnect) {
			process.nextTick(function() {
				callback();
			});
		} else {
			dataSource.connector.connect(callback);
		}
	}
};

exports.SAPoData = SAPoData;

  /**
 * The constructor for SAPoData connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function SAPoData(settings, dataSource) {
	Connector.call(this, 'sapodata', settings);

	this.debug = settings.debug || debug.enabled;

	if (this.debug) {
		debug('Settings: %j', settings);
	}
	this.options = settings;
	this.dataSource = dataSource;
}

util.inherits(SAPoData, Connector);

/**
 * Connect to SAPoData
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Db} db The mongo DB object
 */
SAPoData.prototype.connect = function(callback) {
	var self = this;
	this.client = new Client();
	//this.client.parsers.remove("DEFAULT");
	this.client.parsers.add({
		"name":"DEFAULT",
		"isDefault":true,
		"match":function(response) {
			return true;
		},
		"parse":function(byteBuffer,nrcEventEmitter,parsedCallback) {
			parsedCallback({"error":{"code":"INVALID RESPONSE","message":{"lang":"en","value":"Invalid response"}, body:byteBuffer.toString()}});
		}});

	this.client.on("error", function(err) {
		console.log(err);
	});

	process.nextTick(function() {
		callback && callback(null, self.db);
	});
};

SAPoData.prototype.getTypes = function() {
	return ['sapodata'];
};

/**
 * Disconnect from SAPoData
 */
SAPoData.prototype.disconnect = function(cb) {
	if (this.debug) {
		debug('disconnect');
	}
	if (cb) {
		process.nextTick(cb);
	}
};

/**
 * Disconnect from SAPoData
 */
SAPoData.prototype.ping = function(cb) {
	if (this.debug) {
		debug('ping');
	}
	if (cb) {
		process.nextTick(cb);
	}
};


/*!
 * Convert the data from database to JSON
 *
 * @param {String} model The model name
 * @param {Object} data The data from DB
 */
SAPoData.prototype.fromDatabase = function(model, data) {
	var m = this.getModelDefinition(model);
	var props = m.properties;
	debug("FROM DATABASE");
	if (!data) {
		return null;
	}
	for (var p in props) {
		var prop = props[p];
		if (prop && prop.type === Number && prop.dataType === "Date") {
			var value = data[p];
			if (!value) {
				data[p] = null;
			} else {
				if (value.match(/^\/Date\((\d+?)\)\/$/)) {
					data[p] = parseInt(RegExp.$1, 10);
				}
			}
		}
	}
	data = this.fromDatabaseToPropertyNames(model, data);
	return data;
};

/*!
 * Convert JSON to database-appropriate format
 *
 * @param {String} model The model name
 * @param {Object} data The JSON data to convert
 */
SAPoData.prototype.toDatabase = function(model, data) {
	var m = this.getModelDefinition(model);
	var props = m.properties;
	for (var p in props) {
		var prop = props[p];
		if (data[p] === null || _.isUndefined(data[p])) {
			delete data[p];
		} else if (prop && prop.type === Number && prop.dataType === "Date") {
			var value = data[p];
			if (!value) {
				delete data[p]; //TODO: maybe null???
			} else {
				data[p] = "\/Date("+value+")\/";
			}
		} else if (prop && prop.type === Object) {
			if (typeof data[p] != 'object') {
				//TODO: JSON stringify
			}
		} else if (data[p].toString) {
			data[p] = data[p].toString();
		}
	}
	// Override custom column names
	data = this.fromPropertyToDatabaseNames(model, data);
	debug("TO DATABASE");
	return data;
};


SAPoData.prototype.getToken = function(model, cb) {
	this.execute(model, "findById", {id:'0'}, {headers: {'X-CSRF-Token': 'fetch'}}, function(err, data, resp) {
		var opts = {};
		if (!resp && err) return cb(err);
		if (resp && resp.headers) {
			if (resp.headers["x-csrf-token"]) {
				var token = resp.headers["x-csrf-token"];
				debug("COOKK", resp.headers);
				opts.headers = {
					'X-CSRF-Token': token,
					cookie: resp.headers['set-cookie']
				};
				return cb(null, opts);
			}
		}
		cb("cant get x-csrf-token");
	});
};

SAPoData.prototype.execute = function(model, command) {
	var self = this;
  // Get the parameters for the given command
	var args = [].slice.call(arguments, 2);
	var data = args[0] || {};
	var opts = args[1] || {};
  // The last argument must be a callback function
	var cb = args[args.length - 1];
	var isArray = true;
	var needXRFToken = false;
	var idName = self.idName(model);

	var method;
	var id;
	switch (command) {
	case 'findById':
		isArray = false;
		id = data.id;
		data = undefined;
	case 'find':
		method = 'get';
		break;
	case 'create':
		method = 'post';
		needXRFToken = true;
		isArray = false;
		break;
	case 'update':
		id = data.id;
		method = 'put';
		needXRFToken = true;
		isArray = false;
		break;
	default:
		method = 'get';
	}

	debug("EXEC", model, method, command, data);
	var doExec = function() {
		var m = self.getModelDefinition(model);
		var req = self.buildRequest(m, data, opts, needXRFToken);
		var url = self.buildURL(m, id, needXRFToken);
		debug("EXECUTE", model, url, command, JSON.stringify(req,null, "\t"));
		self.client[method](url, req, function(result, resp) {
			var err = self.handleError(model, result, resp);
			if (err) return cb(err, null, resp);
			var d = self.handleResponse(m, result, isArray);
			cb(null, d, resp);
		}).on('error', function(err, resp) {
			cb(err, null, resp);
		});
	};
	if (needXRFToken) {
		this.getToken(model, function(err, headers) {
			opts = headers;
			doExec();
		});
	} else {
		doExec();
	}

	//callback("NOT YET IMPLEMENTED");
};

SAPoData.prototype.handleError = function(model, data, resp) {
	if (data && data.error) return data.error;
	return null;

};

/**
 * Create a new model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
SAPoData.prototype.create = function(model, data, options, callback) {
	var self = this;
	if (self.debug) {
		debug('create', model, data);
	}
	var idValue = self.getIdValue(model, data);
	var idName = self.idName(model);

	if (idValue === null) {
		delete data[idName]; // Allow SAPoData to generate the id
	}

	data = self.toDatabase(model, data);
	debug("CONVERTED DATA", data);
	this.execute(model, 'create', data, { safe: true }, function(err, result) {
		if (self.debug) {
			debug('create.callback', model, err, result);
		}
		if (err) {
			return callback(err);
		}
		process.nextTick(function() {
			if (result && result[0] && result[0][idName]) {
				callback(err, result[0][idName]);
			}  else {
				callback(err, null);
			}
		});
	});
};

/**
 * Save the model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
SAPoData.prototype.save = function(model, data, options, callback) {
	var self = this;
	if (self.debug) {
		debug('save', model, data);
	}
	var idValue = self.getIdValue(model, data);
	if (!idValue) {
		return this.create(model, data, options, callback);
	} else {
		return this.updateAttributes(model, idValue, data, options, callback);
	}
};

/**
 * Check if a model instance exists by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [callback] The callback function
 *
 */
SAPoData.prototype.exists = function(model, id, options, callback) {
	var self = this;
	if (self.debug) {
		debug('exists', model, id);
	}
	callback("NOT YET IMPLEMENTED");
};


/**
 * Delete a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param [callback] The callback function
 */
SAPoData.prototype.destroy = function destroy(model, id, options, callback) {
	var self = this;
	if (self.debug) {
		debug('delete', model, id);
	}
	callback("NOT YET IMPLEMENTED");
};


SAPoData.prototype.buildWhere = function(model, where) {
	var self = this;
	return where;
};

SAPoData.prototype.buildSort = function(model, order) {
	var sort = {};
	var idName = this.idName(model);
	return order;
	return sort;
};

SAPoData.prototype.getDatabaseColumnName = function(model, propName) {
	if (typeof model === 'string') {
		model = this._models[model];
	}

	if (typeof model !== 'object') {
		return propName; // unknown model type?
	}

	if (typeof model.properties !== 'object') {
		return propName; // missing model properties?
	}

	var prop = model.properties[propName] || {};

	debug('getDatabaseColumnName', propName, prop);

  // Try sap overrides
	if (prop.sapodata) {
		propName = prop.sapodata.fieldName || prop.sapodata.field ||
      prop.sapodata.columnName || prop.sapodata.column ||
      prop.columnName || prop.column || propName;
	} else {
    // Try top level overrides
		propName = prop.columnName || prop.column || propName;
	}

  // Done
  // console.log('->', propName);
	return propName;
};

SAPoData.prototype.convertColumnNames = function(model, data, direction) {
	if (typeof data !== 'object') {
		return data; // skip
	}

	if (typeof model === 'string') {
		model = this._models[model];
	}

	if (typeof model !== 'object') {
		return data; // unknown model type?
	}

	if (typeof model.properties !== 'object') {
		return data; // missing model properties?
	}

	for (var propName in model.properties) {
		var columnName = this.getDatabaseColumnName(model, propName);

    // Copy keys/data if needed
		if (propName === columnName) {
			continue;
		}

		if (direction === 'database') {
			data[columnName] = data[propName];
			delete data[propName];
		}

		if (direction === 'property') {
			data[propName] = data[columnName];
			delete data[columnName];
		}
	}

	return data;
};

SAPoData.prototype.fromPropertyToDatabaseNames = function(model, data) {
	return this.convertColumnNames(model, data, 'database');
};

SAPoData.prototype.fromDatabaseToPropertyNames = function(model, data) {
	return this.convertColumnNames(model, data, 'property');
};

SAPoData.prototype.buildURL = function buildURL(modelDef, id, noFormat) {
	var url = this.options.baseUrl.replace(/\/$/, "");

	url += "/" + modelDef.settings.sapodata.service;
	url += "/" + modelDef.settings.sapodata.resource;
	if (id) {
		url += "('"+id+"')";
	}
	//if (!noFormat)
	//	url += "?$format=json";
	return url;
};

SAPoData.prototype.buildRequest = function(model, data, opts, needXRFToken) {
	if (!opts) opts = {};
	debug("MMMM", opts);
	var req = {
		data: data,
		headers: _.extend({
			"Content-Type": "application/json",
			"Accept": "application/json",
		}, this.options.headers, opts.headers),
		mimetypes: {
			json: ["application/json", "application/json;charset=utf-8"]
		},

		requestConfig: {
			followRedirects:true,//whether redirects should be followed(default,true)
	        maxRedirects:10,//set max redirects allowed (default:21)
	        timeout: 90*1000, //request timeout in milliseconds
	        noDelay: true, //Enable/disable the Nagle algorithm
	        keepAlive: true, //Enable/disable keep-alive functionalityidle socket.
	        keepAliveDelay: 1000 //and optionally set the initial delay before the first keepalive probe is sent
		},
		responseConfig: {
	        timeout: 90*1000 //response timeout
	    }
	};
	if (!needXRFToken) {
		req.parameters = {
			"$format":"json"
		};
		//put all data started with "$" into URL for odata like $filter
		_.each(data, function(v,k) {
			if (k.match(/^\$/)) {
				req.parameters[k] = v;
				delete data[k];
			}
		});
	}
	if (opts.headers) {
		//_.extend(req.headers, opts.headers);
	}
	return req;
};

/**
 * Find matching model instances by the filter
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [callback] The callback function
 */
SAPoData.prototype.all = function all(model, filter, options, cb) {
	var self = this;
	if (self.debug) {
		debug('all', model, filter, options);
	}
	filter = filter || {};
	var idName = self.idName(model);
	var command = 'find';
	var query = {};
	if (filter.where) {
		debug("WHERE", filter.where);
		//IN CASE find by ID
		if (_.size(_.keys(filter.where)) == 1 && filter.where[idName] && filter.limit === 1 && filter.offset == 0) {
			command = 'findById';
			query = {id:filter.where[idName]};
		} else {
			query = self.buildWhere(model, filter.where);
		}
	}
	var fields = filter.fields;

  // Convert custom column names
	fields = self.fromPropertyToDatabaseNames(model, fields);
	this.execute(model, command, query, { safe: true }, function(err, result) {
		if (self.debug) {
			debug('all.callback', model, err);
		}
		if (err) {
			return cb(err);
		}
		result = _.map(result, function(data) {
			//TODO map fields
			return self.fromDatabase(model, data);
		});
		cb(null, result);
	});
};

SAPoData.prototype.handleResponse = function(modelDef, data, isArray) {
	//TODO avoid resourcePath
	var q = modelDef.settings.sapodata.resourcePath || '$.d.results.*';
	if (!isArray) q = '$.d';
	var result = jsonpath.query(data, q);
	if (!isArray) return result;
	return result;
};

/**
 * Delete all instances for the given model
 * @param {String} model The model name
 * @param {Object} [where] The filter for where
 * @param {Function} [callback] The callback function
 */
SAPoData.prototype.destroyAll = function destroyAll(model, where, options, callback) {
	var self = this;
	if (self.debug) {
		debug('destroyAll', model, where);
	}
	if (!callback && 'function' === typeof where) {
		callback = where;
		where = undefined;
	}
	where = self.buildWhere(model, where);
	callback && callback("NOT YET, IMPLEMENTED");
	//callback && callback(err, { count: affectedCount });
};

/**
 * Count the number of instances for the given model
 *
 * @param {String} model The model name
 * @param {Function} [callback] The callback function
 * @param {Object} filter The filter for where
 *
 */
SAPoData.prototype.count = function count(model, where, options, callback) {
	var self = this;
	if (self.debug) {
		debug('count', model, where);
	}
	where = self.buildWhere(model, where);
	callback && callback("NOT YET IMPLEMENTED");
};

/**
 * Replace properties for the model instance data
 * @param {String} model The name of the model
 * @param {*} id The instance id
 * @param {Object} data The model data
 * @param {Object} options The options object
 * @param {Function} [cb] The callback function
 */
SAPoData.prototype.replaceById = function replace(model, id, data, options, cb) {
	if (this.debug) debug('replace', model, id, data);
	callback && callback("NOT YET IMPLEMENTED");
};

/**
 * Update properties for the model instance data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
SAPoData.prototype.updateAttributes = function updateAttrs(model, id, data, options, callback) {
	var self = this;
	if (self.debug) {
		debug('updateAttributes', model, data);
	}
	var idValue = id;
	var idName = self.idName(model);
	self.setIdValue(model, data, idValue);

	data = self.toDatabase(model, data);
	this.execute(model, 'update', data, { safe: true }, function(err, result) {
		if (self.debug) {
			debug('updateAtributes.callback', model, err, result);
		}
		if (err) {
			return callback(err);
		}
		process.nextTick(function() {
			if (result && result[0] && result[0][idName]) {
				callback(err, self.fromDatabase(result[0]));
			}  else {
				callback(err, null);
			}
		});
	});
};

/**
 * Update all matching instances
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
SAPoData.prototype.update =
  SAPoData.prototype.updateAll = function updateAll(model, where, data, options, callback) {
	var self = this;
	if (self.debug) {
		debug('updateAll', model, where, data);
	}
	//TODO: find all
	//TODO: call save attributes to each
	this.all(model, {where: where}, options, function(err, list) {
		if (err) return callback(err);
	});


	var idName = this.idName(model);

	where = self.buildWhere(model, where);
	delete data[idName];

	data = self.toDatabase(model, data);

	//cb && cb(err, { count: affectedCount });
	callback && callback("NOT YET IMPLEMENTED");
};
