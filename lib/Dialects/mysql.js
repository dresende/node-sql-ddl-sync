var SQL = require("../SQL");

exports.hasCollection = function (db, name, cb) {
	db.query("SHOW TABLES LIKE ?", [ name ], function (err, rows) {
		if (err) return cb(err);

		return cb(null, rows.length > 0);
	});
};

exports.getCollectionProperties = function (db, name, cb) {
	db.query("SHOW COLUMNS FROM ??", [ name ], function (err, cols) {
		if (err) return cb(err);

		var columns = {};

		for (var i = 0; i < cols.length; i++) {
			var column = {};

			if (cols[i].Type.indexOf(" ") > 0) {
				cols[i].SubType = cols[i].Type.substr(cols[i].Type.indexOf(" ") + 1).split(/\s+/);
				cols[i].Type = cols[i].Type.substr(0, cols[i].Type.indexOf(" "));
			}

			var m = cols[i].Type.match(/^(.+)\((\d+)\)$/);

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

			switch (cols[i].Type.toUpperCase()) {
				case "SMALLINT":
				case "INT":
				case "BIGINT":
					column.type = "number";
					column.rational = false;
					if (cols[i].Size) {
						column.size = cols[i].Size;
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
				case "FLOAT":
					column.type = "number";
					column.rational = true;
					break;
				case "VARCHAR":
					column.type = "text";
					if (cols[i].Size) {
						column.size = cols[i].Size;
					}
					break;
				default:
					return cb(new Error("Unknown column type '" + cols[i].Type + "'"));
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

exports.escapeId = function () {
	return Array.prototype.slice.apply(arguments).map(function (el) {
		return "`" + el.replace(/`/g, '``') + "`";
	}).join(".");
};

exports.getType = function (property) {
	var type = false;

	switch (property.type) {
		case "text":
			type = "VARCHAR(" + (property.size || 255) + ")";
			break;
		case "number":
			if (property.rational) {
				type = "FLOAT";
			} else {
				type = "INT(5)";
			}
			break;
		case "boolean":
			type = "TINYINT(1)";
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

	return type;
};
