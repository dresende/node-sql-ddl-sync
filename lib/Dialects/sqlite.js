var Queue = require("../Queue").Queue;
var SQL   = require("../SQL");
var util  = require("util");

exports.hasCollection = function (db, name, cb) {
	db.all("SELECT * FROM sqlite_master " +
	       "WHERE type = 'table' and name = ?",
	       [ name ],
	function (err, rows) {
		if (err) return cb(err);

		return cb(null, rows.length > 0);
	});
};

exports.getCollectionProperties = function (db, name, cb) {
	db.all("PRAGMA table_info(" + exports.escapeId(name) + ")", function (err, cols) {
		if (err) return cb(err);

		var columns = {}, m;

		for (var i = 0; i < cols.length; i++) {
			var column = {};

			if (cols[i].pk) {
				column.primary = true;
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

exports.createCollection = function (db, name, columns, primary, cb) {
	return db.all(SQL.CREATE_TABLE({
		name    : name,
		columns : columns,
		primary : primary
	}, exports), cb);
};

exports.dropCollection = function (db, name, cb) {
	return db.query(SQL.DROP_TABLE({
		name    : name
	}, exports), cb);
};

exports.addCollectionColumn = function (db, name, column, after_column, cb) {
	return db.all(SQL.ALTER_TABLE_ADD_COLUMN({
		name   : name,
		column : column,
		after  : after_column,
		first  : !after_column
	}, exports), cb);
};

exports.modifyCollectionColumn = function (db, name, column, cb) {
	return db.all(SQL.ALTER_TABLE_MODIFY_COLUMN({
		name        : name,
		column      : column
	}, exports), cb);
};

exports.dropCollectionColumn = function (db, name, column, cb) {
	// sqlite does not support dropping columns
	return cb();
};

exports.getCollectionIndexes = function (db, name, cb) {
	db.all("PRAGMA index_list(" + exports.escapeId(name) + ")", function (err, rows) {
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
				db.all("PRAGMA index_info(" + exports.escapeVal(k) + ")", function (err, rows) {
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

exports.addIndex = function (db, name, unique, collection, columns, cb) {
	return db.all(SQL.CREATE_INDEX({
		name       : name,
		unique     : unique,
		collection : collection,
		columns    : columns
	}, exports), cb);
};

exports.removeIndex = function (db, name, collection, cb) {
	return db.all("DROP INDEX IF EXISTS " + exports.escapeId(name), cb);
};

exports.checkPrimary = function (primary) {
	if (primary.length === 1) {
		return [];
	}

	return primary;
};

exports.supportsType = function (type) {
	switch (type) {
		case "boolean":
		case "enum":
			return "number";
	}
	return type;
};

exports.getType = function (collection, name, property) {
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
			property.primary = true;
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
	if (property.primary) {
		if (!property.required) {
			// append if not set
			type += " NOT NULL";
		}
		if (property.serial) {
			type += " PRIMARY KEY";
		}
	}
	if (property.serial) {
		if (!property.primary) {
			type += " PRIMARY KEY";
		}
		type += " AUTOINCREMENT";
	}
	if (property.hasOwnProperty("defaultValue")) {
		type += " DEFAULT " + exports.escapeVal(property.defaultValue);
	}

	return {
		value  : type,
		before : false
	};
};

exports.escapeId = function () {
	return Array.prototype.slice.apply(arguments).map(function (el) {
		return "`" + el.replace(/`/g, '``') + "`";
	}).join(".");
};

exports.escapeVal = function (val, timeZone) {
	if (val === undefined || val === null) {
		return 'NULL';
	}

	if (Array.isArray(val)) {
		if (val.length === 1 && Array.isArray(val[0])) {
			return "(" + val[0].map(exports.escapeVal.bind(this)) + ")";
		}
		return "(" + val.map(exports.escapeVal.bind(this)).join(", ") + ")";
	}

	if (util.isDate(val)) {
		return "'" + dateToString(val, timeZone || "local") + "'";
	}

	if (Buffer.isBuffer(val)) {
		return "X'" + val.toString("hex") + "'";
	}

	switch (typeof val) {
		case "number":
			if (!isFinite(val)) {
				val = val.toString();
				break;
			}
			return val;
		case "boolean":
			return val ? 1 : 0;
		case "function":
			return val(exports);
	}

	// No need to escape backslashes with default PostgreSQL 9.1+ config.
	// Google 'postgresql standard_conforming_strings' for details.
	return "'" + val.replace(/\'/g, "''") + "'";
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
