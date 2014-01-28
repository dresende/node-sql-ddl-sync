var Queue = require("../Queue").Queue;
var SQL   = require("../SQL");
var util  = require("util");

exports.hasCollection = function (driver, name, cb) {
	driver.execQuery("SELECT * FROM sqlite_master " +
	       "WHERE type = 'table' and name = ?",
	       [ name ],
	function (err, rows) {
		if (err) return cb(err);

		return cb(null, rows.length > 0);
	});
};

exports.getCollectionProperties = function (driver, name, cb) {
	driver.execQuery("PRAGMA table_info(" + driver.query.escapeId(name) + ")", function (err, cols) {
		if (err) return cb(err);

		var columns = {}, m;

		for (var i = 0; i < cols.length; i++) {
			var column = {};

			if (cols[i].pk) {
				column.key = true;
			}

			if (cols[i].notnull) {
				column.required = true;
			}
			if (cols[i].dflt_value) {
				m = cols[i].dflt_value.match(/^'(.*)'$/);
				if (m) {
					column.defaultValue = m[1];
				} else {
					column.defaultValue = m[0];
				}
			}

			switch (cols[i].type.toUpperCase()) {
				case "INTEGER":
					column.type = "number";
					column.rational = false;
					break;
				case "REAL":
					column.type = "number";
					column.rational = true;
					break;
				case "DATETIME":
					column.type = "date";
					column.time = true;
					break;
				case "BLOB":
					column.type = "binary";
					column.big = true;
					break;
				case "TEXT":
					column.type = "text";
					break;
				default:
					return cb(new Error("Unknown column type '" + cols[i].Type + "'"));
			}

			columns[cols[i].name] = column;
		}

		return cb(null, columns);
	});
};

exports.createCollection = function (driver, name, columns, keys, cb) {
	return driver.execQuery(SQL.CREATE_TABLE({
		name    : name,
		columns : columns,
		keys    : keys
	}, driver), cb);
};

exports.dropCollection = function (driver, name, cb) {
	return driver.execQuery(SQL.DROP_TABLE({
		name    : name
	}, driver), cb);
};

exports.addCollectionColumn = function (driver, name, column, after_column, cb) {
	return driver.execQuery(SQL.ALTER_TABLE_ADD_COLUMN({
		name   : name,
		column : column,
		after  : after_column,
		first  : !after_column
	}, driver), cb);
};

exports.modifyCollectionColumn = function (driver, name, column, cb) {
	return driver.execQuery(SQL.ALTER_TABLE_MODIFY_COLUMN({
		name        : name,
		column      : column
	}, driver), cb);
};

exports.dropCollectionColumn = function (driver, name, column, cb) {
	// sqlite does not support dropping columns
	return cb();
};

exports.getCollectionIndexes = function (driver, name, cb) {
	driver.execQuery("PRAGMA index_list(" + driver.query.escapeId(name) + ")", function (err, rows) {
		if (err) return cb(err);

		var indexes = convertIndexRows(rows);
		var queue   = new Queue(function (err) {
			return cb(err, indexes);
		});

		for (var k in indexes) {
			if (k.match(/^sqlite_autoindex/)) {
				delete indexes[k];
				continue;
			}
			queue.add(k, function (k, next) {
				driver.execQuery("PRAGMA index_info(" + driver.query.escapeVal(k) + ")", function (err, rows) {
					if (err) return next(err);

					for (var i = 0; i < rows.length; i++) {
						indexes[k].columns.push(rows[i].name);
					}

					return next();
				});
			});
		}

		return queue.check();
	});
};

exports.addIndex = function (driver, name, unique, collection, columns, cb) {
	return driver.execQuery(SQL.CREATE_INDEX({
		name       : name,
		unique     : unique,
		collection : collection,
		columns    : columns
	}, driver), cb);
};

exports.removeIndex = function (driver, name, collection, cb) {
	return driver.execQuery("DROP INDEX IF EXISTS " + driver.query.escapeId(name), cb);
};

exports.processKeys = function (keys) {
	if (keys.length === 1) {
		return [];
	}

	return keys;
};

exports.supportsType = function (type) {
	switch (type) {
		case "boolean":
		case "enum":
			return "number";
	}
	return type;
};

exports.getType = function (collection, name, property, driver) {
	var type = false;

	switch (property.type) {
		case "text":
			type = "TEXT";
			break;
		case "number":
			if (property.rational) {
				type = "REAL";
			} else {
				type = "INTEGER";
			}
			break;
		case "serial":
			property.serial = true;
			property.key = true;
			type = "INTEGER";
			break;
		case "boolean":
			type = "INTEGER";
			break;
		case "date":
			type = "DATETIME";
			break;
		case "binary":
		case "object":
			type = "BLOB";
			break;
		case "enum":
			type = "INTEGER";
			break;
		case "point":
			type = "POINT";
			break;
	}

	if (!type) return false;

	if (property.required) {
		type += " NOT NULL";
	}
	if (property.key) {
		if (!property.required) {
			// append if not set
			type += " NOT NULL";
		}
		if (property.serial) {
			type += " PRIMARY KEY";
		}
	}
	if (property.serial) {
		if (!property.key) {
			type += " PRIMARY KEY";
		}
		type += " AUTOINCREMENT";
	}
	if (property.hasOwnProperty("defaultValue")) {
		type += " DEFAULT " + driver.query.escapeVal(property.defaultValue);
	}

	return {
		value  : type,
		before : false
	};
};

function convertIndexRows(rows) {
	var indexes = {};

	for (var i = 0; i < rows.length; i++) {
		if (!indexes.hasOwnProperty(rows[i].name)) {
			indexes[rows[i].name] = {
				columns : [],
				unique  : (rows[i].unique == 1)
			};
		}
	}

	return indexes;
}

function dateToString(date, timeZone) {
	var dt = new Date(date);

	if (timeZone != 'local') {
		var tz = convertTimezone(timeZone);

		dt.setTime(dt.getTime() + (dt.getTimezoneOffset() * 60000));
		if (tz !== false) {
			dt.setTime(dt.getTime() + (tz * 60000));
		}
	}

	var year   = dt.getFullYear();
	var month  = zeroPad(dt.getMonth() + 1);
	var day    = zeroPad(dt.getDate());
	var hour   = zeroPad(dt.getHours());
	var minute = zeroPad(dt.getMinutes());
	var second = zeroPad(dt.getSeconds());
	var milli  = zeroPad(dt.getMilliseconds(), 3);

	return year + '-' + month + '-' + day + 'T' + hour + ':' + minute + ':' + second + '.' + milli + 'Z';
}

function zeroPad(number, n) {
	if (arguments.length == 1) n = 2;

	number = "" + number;

	while (number.length < n) {
		number = "0" + number;
	}
	return number;
}

function convertTimezone(tz) {
	if (tz == "Z") return 0;

	var m = tz.match(/([\+\-\s])(\d\d):?(\d\d)?/);
	if (m) {
		return (m[1] == '-' ? -1 : 1) * (parseInt(m[2], 10) + ((m[3] ? parseInt(m[3], 10) : 0) / 60)) * 60;
	}
	return false;
}
