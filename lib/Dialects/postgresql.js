var util        = require("util");
var SQL         = require("../SQL");
var Queue       = require("../Queue").Queue;
var columnSizes = {
	integer:  { 2: 'SMALLINT', 4: 'INTEGER', 8: 'BIGINT' },
	floating: {                4: 'REAL',    8: 'DOUBLE PRECISION' }
};

exports.hasCollection = function (db, name, cb) {
	db.query("SELECT * FROM information_schema.tables WHERE table_name = $1", [ name ], function (err, result) {
		if (err) return cb(err);

		return cb(null, result.rows.length > 0);
	});
};

exports.getCollectionProperties = function (db, name, cb) {
	db.query("SELECT * FROM information_schema.columns WHERE table_name = $1", [ name ], function (err, result) {
		if (err) return cb(err);

		var cols    = result.rows;
		var columns = {}, m;

		for (var i = 0; i < cols.length; i++) {
			var column = {};

			if (cols[i].is_nullable.toUpperCase() == "NO") {
				column.required = true;
			}
			if (cols[i].column_default !== null) {
				m = cols[i].column_default.match(/^'(.+)'::/);
				if (m) {
					column.defaultValue = m[1];
				} else {
					column.defaultValue = cols[i].column_default;
				}
			}

			switch (cols[i].data_type.toUpperCase()) {
				case "SMALLINT":
				case "INTEGER":
				case "BIGINT":
					column.type = "number";
					column.rational = false;
					for (var k in columnSizes.integer) {
						if (columnSizes.integer[k] == cols[i].data_type.toUpperCase()) {
							column.size = k;
							break;
						}
					}
					break;
				case "REAL":
				case "DOUBLE PRECISION":
					column.type = "number";
					column.rational = true;
					for (var k in columnSizes.floating) {
						if (columnSizes.floating[k] == cols[i].data_type.toUpperCase()) {
							column.size = k;
							break;
						}
					}
					break;
				case "BOOLEAN":
					column.type = "boolean";
					break;
				case "TIMESTAMP WITHOUT TIME ZONE":
					column.time = true;
				case "DATE":
					column.type = "date";
					break;
				case "BYTEA":
					column.type = "binary";
					break;
				case "CHARACTER VARYING":
					column.type = "text";
					if (cols[i].character_maximum_length) {
						column.size = cols[i].character_maximum_length;
					}
					break;
				case "USER-DEFINED":
					if (cols[i].udt_name.match(/_enum_/)) {
						column.type = "enum";
						column.values = [];
						break;
					}
				default:
					console.log(cols[i]);
					return cb(new Error("Unknown column type '" + cols[i].data_type + "'"));
			}

			columns[cols[i].column_name] = column;
		}

		return checkColumnTypes(db, name, columns, cb);
	});
};

exports.createCollection = function (db, name, columns, primary, cb) {
	return db.query(SQL.CREATE_TABLE({
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
	var sql = "ALTER TABLE " + exports.escapeId(name) + " ADD " + column;

	return db.query(sql, cb);
};

exports.modifyCollectionColumn = function (db, name, column, cb) {
	var p        = column.indexOf(" ");
	var col_name = column.substr(0, p);
	var queue    = new Queue(cb);
	var col_type, m;

	column = column.substr(p + 1);

	p = column.indexOf(" ");
	if (p > 0) {
		col_type = column.substr(0, p);
		column = column.substr(p + 1);
	} else {
		col_type = column;
		column = false;
	}

	queue.add(function (next) {
		return db.query("ALTER TABLE " + name +
		                " ALTER " + col_name +
		                " TYPE " + col_type, next);
	});

	if (column) {
		if (column.match(/NOT NULL/)) {
			queue.add(function (next) {
				return db.query("ALTER TABLE " + name +
				                " ALTER " + col_name +
				                " SET NOT NULL", next);
			});
		} else {
			queue.add(function (next) {
				return db.query("ALTER TABLE " + name +
				                " ALTER " + col_name +
				                " DROP NOT NULL", next);
			});
		}

		if (m = column.match(/DEFAULT (.+)$/)) {
			queue.add(function (next) {
				return db.query("ALTER TABLE " + name +
				                " ALTER " + col_name +
				                " SET DEFAULT " + m[1], next);
			});
		}
	}

	return queue.check();
};

exports.dropCollectionColumn = function (db, name, column, cb) {
	return db.query(SQL.ALTER_TABLE_DROP_COLUMN({
		name        : name,
		column      : column
	}, exports), cb);
};

exports.getCollectionIndexes = function (db, name, cb) {
	db.query("SELECT t.relname, i.relname, a.attname, ix.indisunique, ix.indisprimary " +
	         "FROM pg_class t, pg_class i, pg_index ix, pg_attribute a " +
	         "WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid " +
	         "AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) " +
	         "AND t.relkind = 'r' AND t.relname = $1",
	         [ name ],
	function (err, result) {
		if (err) return cb(err);

		return cb(null, convertIndexRows(result.rows));
	});
};

exports.addIndex = function (db, name, unique, collection, columns, cb) {
	return db.query(SQL.CREATE_INDEX({
		name       : name,
		unique     : unique,
		collection : collection,
		columns    : columns
	}, exports), cb);
};

exports.removeIndex = function (db, name, collection, cb) {
	return db.query("DROP INDEX " + exports.escapeId(name), cb);
};

exports.convertIndexes = function (collection, indexes) {
	for (var i = 0; i < indexes.length; i++) {
		indexes[i].name = collection.name + "_" + indexes[i].name;
	}

	return indexes;
};

exports.getType = function (collection, name, property) {
	var type   = false;
	var before = false;

	if (property.serial) {
		type = "SERIAL";
	} else {
		switch (property.type) {
			case "text":
				type = "TEXT";
				break;
			case "number":
				if (property.rational) {
					type = columnSizes.floating[property.size || 4];
				} else {
					type = columnSizes.integer[property.size || 4];
				}
				break;
			case "serial":
				property.serial = true;
				type = "SERIAL";
				break;
			case "boolean":
				type = "BOOLEAN";
				break;
			case "date":
				if (!property.time) {
					type = "DATE";
				} else {
					type = "TIMESTAMP WITHOUT TIME ZONE";
				}
				break;
			case "binary":
			case "object":
				type = "BYTEA";
				break;
			case "enum":
				type   = collection + "_enum_" + name.toLowerCase();
				before = function (db, cb) {
					var type = collection + "_enum_" + name.toLowerCase();

					db.query("SELECT * FROM pg_catalog.pg_type WHERE typname = $1", [ type ], function (err, result) {
						if (!err && result.rows.length) {
							return cb();
						}

						var values = property.values.map(function (val) {
							return exports.escapeVal(val);
						});

						return db.query("CREATE TYPE " + type + " " +
						                "AS ENUM (" + values + ")", cb);
					});
				};

				// return {
				// 	value  : type,
				// 	before : before
				// };
				break;
			case "point":
				type = "POINT";
				break;
		}

		if (!type) return false;

		if (property.required) {
			type += " NOT NULL";
		}
		if (property.hasOwnProperty("defaultValue")) {
			type += " DEFAULT " + exports.escapeVal(property.defaultValue);
		}
	}

	return {
		value  : type,
		before : before
	};
};

exports.escapeId = function () {
	return Array.prototype.slice.apply(arguments).map(function (el) {
		return el.split(".").map(function (ele) {
			return "\"" + ele.replace(/\"/g, "\"\"") + "\"";
		}).join(".");
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
		return "'\\x" + val.toString("hex") + "'";
	}

	switch (typeof val) {
		case "number":
			if (!isFinite(val)) {
				val = val.toString();
				break;
			}
			return val;
		case "boolean":
			return val ? "true" : "false";
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
		if (rows[i].indisprimary) {
			continue;
		}

		if (!indexes.hasOwnProperty(rows[i].relname)) {
			indexes[rows[i].relname] = {
				columns : [],
				unique  : rows[i].indisunique
			};
		}

		indexes[rows[i].relname].columns.push(rows[i].attname);
	}

	return indexes;
}

function checkColumnTypes(db, collection, columns, cb) {
	var queue = new Queue(function () {
		return cb(null, columns);
	});

	for (var k in columns) {
		if (columns[k].type == "enum") {
			queue.add(k, columns[k], function (name, col, next) {
				var col_name = collection + "_enum_" + name;

				db.query("SELECT t.typname, string_agg(e.enumlabel, '|' ORDER BY e.enumsortorder) AS enum_values " +
				         "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid  " +
				         "WHERE t.typname = $1 GROUP BY 1", [ col_name ],
				function (err, result) {
					if (err) {
						return next(err);
					}
					if (result.rows.length) {
						col.values = result.rows[0].enum_values.split("|");
					}

					return next();
				});
			});
		}
	}

	return queue.check();
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
