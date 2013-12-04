var SQL  = require("../SQL");
var util = require("util");

var columnSizes = {
	integer:  { 2: 'SMALLINT', 4: 'INTEGER', 8: 'BIGINT' },
	floating: {                4: 'FLOAT',   8: 'DOUBLE' }
};

exports.hasCollection = function (db, name, cb) {
	db.query("SHOW TABLES LIKE ?", [ name ], function (err, rows) {
		if (err) return cb(err);

		return cb(null, rows.length > 0);
	});
};

exports.getCollectionProperties = function (db, name, cb) {
	db.query("SHOW COLUMNS FROM ??", [ name ], function (err, cols) {
		if (err) return cb(err);

		var columns = {}, m;

		for (var i = 0; i < cols.length; i++) {
			var column = {};

			if (cols[i].Type.indexOf(" ") > 0) {
				cols[i].SubType = cols[i].Type.substr(cols[i].Type.indexOf(" ") + 1).split(/\s+/);
				cols[i].Type = cols[i].Type.substr(0, cols[i].Type.indexOf(" "));
			}

			m = cols[i].Type.match(/^(.+)\((\d+)\)$/);
			if (m) {
				cols[i].Size = parseInt(m[2], 10);
				cols[i].Type = m[1];
			}

			if (cols[i].Extra.toUpperCase() == "AUTO_INCREMENT") {
				column.serial = true;
				column.unsigned = true;
			}

			if (cols[i].Key == "PRI") {
				column.primary = true;
			}

			if (cols[i].Null.toUpperCase() == "NO") {
				column.required = true;
			}
			if (cols[i].Default !== null) {
				column.defaultValue = cols[i].Default;
			}

			switch (cols[i].Type.toUpperCase()) {
				case "SMALLINT":
				case "INTEGER":
				case "BIGINT":
				case "INT":
					column.type = "number";
					column.rational = false;
					column.size = 4; // INT
					for (var k in columnSizes.integer) {
						if (columnSizes.integer[k] == cols[i].Type.toUpperCase()) {
							column.size = k;
							break;
						}
					}
					break;
				case "FLOAT":
				case "DOUBLE":
					column.type = "number";
					column.rational = true;
					for (var k in columnSizes.floating) {
						if (columnSizes.floating[k] == cols[i].Type.toUpperCase()) {
							column.size = k;
							break;
						}
					}
					break;
				case "TINYINT":
					if (cols[i].Size == 1) {
						column.type = "boolean";
					} else {
						column.type = "number";
						column.rational = false;
					}
					break;
				case "DATETIME":
					column.time = true;
				case "DATE":
					column.type = "date";
					break;
				case "LONGBLOB":
					column.big = true;
				case "BLOB":
					column.type = "binary";
					break;
				case "VARCHAR":
					column.type = "text";
					if (cols[i].Size) {
						column.size = cols[i].Size;
					}
					break;
				default:
					m = cols[i].Type.match(/^enum\('(.+)'\)$/);
					if (m) {
						column.type = "enum";
						column.values = m[1].split(/'\s*,\s*'/);
						break;
					}
					return cb(new Error("Unknown column type '" + cols[i].Type + "'"));
			}

			if (column.serial) {
				column.type = "serial";
			}

			columns[cols[i].Field] = column;
		}

		return cb(null, columns);
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
	return db.query(SQL.ALTER_TABLE_ADD_COLUMN({
		name   : name,
		column : column,
		after  : after_column,
		first  : !after_column
	}, exports), cb);
};

exports.modifyCollectionColumn = function (db, name, column, cb) {
	return db.query(SQL.ALTER_TABLE_MODIFY_COLUMN({
		name        : name,
		column      : column
	}, exports), cb);
};

exports.dropCollectionColumn = function (db, name, column, cb) {
	return db.query(SQL.ALTER_TABLE_DROP_COLUMN({
		name        : name,
		column      : column
	}, exports), cb);
};

exports.getCollectionIndexes = function (db, name, cb) {
	db.query("SHOW INDEX FROM ??", [ name ], function (err, rows) {
		if (err) return cb(err);

		return cb(null, convertIndexRows(rows));
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
	return db.query(SQL.DROP_INDEX({
		name       : name,
		collection : collection
	}, exports), cb);
};

exports.getType = function (collection, name, property) {
	var type = false;

	switch (property.type) {
		case "text":
			if (property.big) {
				type = "LONGTEXT";
			} else {
				type = "VARCHAR(" + Math.min(Math.max(parseInt(property.size, 10) || 255, 1), 65535) + ")";
			}
			break;
		case "number":
			if (property.rational) {
				type = columnSizes.floating[property.size || 4];
			} else {
				type = columnSizes.integer[property.size || 4];
			}
			break;
		case "serial":
			property.type = "number";
			property.serial = true;
			property.primary = true;
			type = "INT(11)";
			break;
		case "boolean":
			type = "TINYINT(1)";
			break;
		case "date":
			if (!property.time) {
				type = "DATE";
			} else {
				type = "DATETIME";
			}
			break;
		case "binary":
		case "object":
			if (property.big === true) {
				type = "LONGBLOB";
			} else {
				type = "BLOB";
			}
			break;
		case "enum":
			type = "ENUM (" + property.values.map(exports.escapeVal) + ")";
			break;
		case "point":
			type = "POINT";
			break;
	}

	if (!type) return false;

	if (property.required) {
		type += " NOT NULL";
	}
	if (property.serial) {
		if (!property.required) {
			// append if not set
			type += " NOT NULL";
		}
		type += " AUTO_INCREMENT";
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

	if (Buffer.isBuffer(val)) {
		return bufferToString(val);
	}

	if (Array.isArray(val)) {
		return arrayToList(val, timeZone || "local");
	}

	if (util.isDate(val)) {
		val = dateToString(val, timeZone || "local");
	} else {
		switch (typeof val) {
			case 'boolean':
				return (val) ? 'true' : 'false';
			case 'number':
				if (!isFinite(val)) {
					val = val.toString();
					break;
				}
				return val + '';
			case "object":
				return objectToValues(val, timeZone || "local");
			case "function":
				return val(exports);
		}
	}

	val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
		switch(s) {
			case "\0": return "\\0";
			case "\n": return "\\n";
			case "\r": return "\\r";
			case "\b": return "\\b";
			case "\t": return "\\t";
			case "\x1a": return "\\Z";
			default: return "\\" + s;
		}
	});

	return "'" + val + "'";
};

function convertIndexRows(rows) {
	var indexes = {};

	for (var i = 0; i < rows.length; i++) {
		if (rows[i].Key_name == 'PRIMARY') {
			continue;
		}
		if (!indexes.hasOwnProperty(rows[i].Key_name)) {
			indexes[rows[i].Key_name] = {
				columns : [],
				unique  : (rows[i].Non_unique == 0)
			};
		}

		indexes[rows[i].Key_name].columns.push(rows[i].Column_name);
	}

	return indexes;
}

function objectToValues(object, timeZone) {
	var values = [];
	for (var key in object) {
		var value = object[key];

		if(typeof value === 'function') {
			continue;
		}

		values.push(exports.escapeId(key) + ' = ' + exports.escapeVal(value, timeZone));
	}

	return values.join(', ');
}

function arrayToList(array, timeZone) {
	return "(" + array.map(function(v) {
		if (Array.isArray(v)) return arrayToList(v);
		return exports.escapeVal(v, timeZone);
	}).join(', ') + ")";
}

function bufferToString(buffer) {
	var hex = '';

	try {
		hex = buffer.toString('hex');
	} catch (err) {
		// node v0.4.x does not support hex / throws unknown encoding error
		for (var i = 0; i < buffer.length; i++) {
			var b = buffer[i];
			hex += zeroPad(b.toString(16));
		}
	}

	return "X'" + hex+ "'";
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

	return year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second;
}

function zeroPad(number) {
	return (number < 10) ? '0' + number : number;
}

function convertTimezone(tz) {
	if (tz == "Z") return 0;

	var m = tz.match(/([\+\-\s])(\d\d):?(\d\d)?/);
	if (m) {
		return (m[1] == '-' ? -1 : 1) * (parseInt(m[2], 10) + ((m[3] ? parseInt(m[3], 10) : 0) / 60)) * 60;
	}
	return false;
}
