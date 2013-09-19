var _ = require("lodash");
var noOp = function () {};

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

		for (var k in collection.properties) {
			var col = createColumn(k, collection.properties[k]);

			if (col === false) {
				return cb(new Error("Unknown type for property '" + k + "'"));
			}

			if (collection.properties[k].primary) {
				primary.push(k);
			}

			columns.push(col);
		}

		debug("Creating " + collection.name);

		return Dialect.createCollection(db, collection.name, columns, primary, cb);
	};

	var createColumn = function (name, property) {
		var type = Dialect.getType(property);

		if (type === false) {
			return false;
		}

		return Dialect.escapeId(name) + " " + type;
	};

	var syncCollection = function (collection, columns, cb) {
		var last_k      = null;
		var pending     = 0;
		var donePending = function (err) {
			if (pending < 0) return;
			if (err) {
				pending = -1; // error state
				return cb(err);
			}
			if (--pending === 0) {
				return cb();
			}
		};

		debug("Synchronizing " + collection.name);

		for (var k in collection.properties) {
			if (!columns.hasOwnProperty(k)) {
				var col = createColumn(k, collection.properties[k]);

				if (col === false) {
					return cb(new Error("Unknown type for property '" + k + "'"));
				}

				pending += 1;

				debug("Adding column " + collection.name + "." + k + ": " + col);
				Dialect.addCollectionColumn(db, collection.name, col, last_k, donePending);
			} else if (needToSync(collection.properties[k], columns[k])) {
				var col = createColumn(k, collection.properties[k]);

				if (col === false) {
					return cb(new Error("Unknown type for property '" + k + "'"));
				}

				pending += 1;

				debug("Modifying column " + collection.name + "." + k + ": " + col);
				Dialect.modifyCollectionColumn(db, collection.name, col, donePending);
			}

			last_k = k;
		}

		for (var k in columns) {
			if (!collection.properties.hasOwnProperty(k)) {
				pending += 1;

				debug("Dropping column " + collection.name + "." + k);
				Dialect.dropCollectionColumn(db, collection.name, k, donePending);
			}
		}

		if (pending === 0) {
			return cb();
		}
	};

	var needToSync = function (property, column) {
		if (property.type != column.type) {
			return true;
		}

		if (property.required != column.required && !property.primary) {
			return true;
		}

		if (property.type == "number") {
			if (property.hasOwnProperty("rational") && property.rational != column.rational) {
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
