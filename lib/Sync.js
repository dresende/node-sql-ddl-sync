var Queue = require("./Queue").Queue;
var noOp  = function () {};
var _     = require("lodash");

exports.Sync = Sync;

function Sync(options) {
	var Dialect     = require("./Dialects/" + options.dialect);
	var debug       = options.debug || noOp;
	var db          = options.db;
	var collections = [];

	var processCollection = function (collection, cb) {
		Dialect.hasCollection(db, collection.name, function (err, has) {
			if (err) {
				return cb(err);
			}

			if (!has) {
				return createCollection(collection, cb);
			}

			Dialect.getCollectionProperties(db, collection.name, function (err, columns) {
				if (err) {
					return cb(err);
				}

				return syncCollection(collection, columns, cb);
			});
		});
	};

	var createCollection = function (collection, cb) {
		var columns = [];
		var primary = [];
		var before  = [];
		var nextBefore = function () {
			if (before.length === 0) {
				return Dialect.createCollection(db, collection.name, columns, primary, cb);
			}

			var next = before.shift();

			next(db, function (err) {
				if (err) {
					return cb(err);
				}

				return nextBefore();
			});
		};

		for (var k in collection.properties) {
			var col = createColumn(collection.name, k, collection.properties[k]);

			if (col === false) {
				return cb(new Error("Unknown type for property '" + k + "'"));
			}

			if (collection.properties[k].primary) {
				primary.push(k);
			}

			columns.push(col.value);

			if (col.before) {
				before.push(col.before);
			}
		}

		debug("Creating " + collection.name);

		return nextBefore();
	};

	var createColumn = function (collection, name, property) {
		var type = Dialect.getType(collection, name, property);

		if (type === false) {
			return false;
		}

		return {
			value  : Dialect.escapeId(name) + " " + type.value,
			before : type.before
		};
	};

	var syncCollection = function (collection, columns, cb) {
		var queue   = new Queue(cb);
		var last_k  = null;

		debug("Synchronizing " + collection.name);

		for (var k in collection.properties) {
			if (!columns.hasOwnProperty(k)) {
				var col = createColumn(collection.name, k, collection.properties[k]);

				if (col === false) {
					return cb(new Error("Unknown type for property '" + k + "'"));
				}

				debug("Adding column " + collection.name + "." + k + ": " + col.value);

				if (col.before) {
					queue.add(col, function (col, next) {
						col.before(db, function (err) {
							if (err) {
								return next(err);
							}
							return Dialect.addCollectionColumn(db, collection.name, col.value, last_k, next);
						});
					});
				} else {
					queue.add(function (next) {
						return Dialect.addCollectionColumn(db, collection.name, col.value, last_k, next);
					});
				}
			} else if (needToSync(collection.properties[k], columns[k])) {
				var col = createColumn(collection.name, k, collection.properties[k]);

				if (col === false) {
					return cb(new Error("Unknown type for property '" + k + "'"));
				}

				debug("Modifying column " + collection.name + "." + k + ": " + col.value);

				if (col.before) {
					queue.add(col, function (col, next) {
						col.before(db, function (err) {
							if (err) {
								return next(err);
							}
							return Dialect.modifyCollectionColumn(db, collection.name, col.value, next);
						});
					});
				} else {
					queue.add(function (next) {
						return Dialect.modifyCollectionColumn(db, collection.name, col.value, next);
					});
				}
			}

			last_k = k;
		}

		for (var k in columns) {
			if (!collection.properties.hasOwnProperty(k)) {
				queue.add(function (next) {
					debug("Dropping column " + collection.name + "." + k);
					return Dialect.dropCollectionColumn(db, collection.name, k, next);
				});
			}
		}

		return queue.check();
	};

	var needToSync = function (property, column) {
		if (property.type != column.type) {
			return true;
		}
		if (property.required != column.required && !property.primary) {
			return true;
		}
		if (property.hasOwnProperty("defaultValue") && property.defaultValue != column.defaultValue) {
			return true;
		}
		if (property.type == "number") {
			if ((property.size || 4) != column.size) {
				return true;
			}
			if (property.hasOwnProperty("rational") && property.rational != column.rational) {
				return true;
			}
		}
		if (property.type == "enum") {
			if (_.difference(property.values, column.values).length > 0
			|| _.difference(column.values, property.values).length > 0) {
				return true;
			}
		}

		return false;
	};

	return {
		define : function (collection, properties) {
			collections.push({
				name       : collection,
				properties : properties
			});
		},
		sync : function (cb) {
			var i = 0;
			var processNext = function () {
				if (i >= collections.length) {
					return cb();
				}

				var collection = collections[i++];

				processCollection(collection, function (err) {
					if (err) {
						return cb(err);
					}

					return processNext();
				});
			};

			return processNext();
		}
	}
}
