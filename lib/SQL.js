exports.CREATE_TABLE = function (options, dialect) {
	var sql = "CREATE TABLE " + dialect.escapeId(options.name) + " (" + options.columns.join(", ");

	if (options.primary && options.primary.length > 0) {
		sql += ", PRIMARY KEY (" + options.primary.map(dialect.escapeId).join(", ") + ")";
	}

	sql += ")";

	return sql;
};

exports.ALTER_TABLE_ADD_COLUMN = function (options, dialect) {
	var sql = "ALTER TABLE " + dialect.escapeId(options.name) +
	          " ADD " + options.column;

	if (options.after) {
		sql += " AFTER " + dialect.escapeId(options.after);
	} else if (options.first) {
		sql += " FIRST";
	}

	return sql;
};

exports.ALTER_TABLE_MODIFY_COLUMN = function (options, dialect) {
	var sql = "ALTER TABLE " + dialect.escapeId(options.name) +
	          " MODIFY " + options.column;

	return sql;
};

exports.ALTER_TABLE_DROP_COLUMN = function (options, dialect) {
	var sql = "ALTER TABLE " + dialect.escapeId(options.name) +
	          " DROP " + dialect.escapeId(options.column);

	return sql;
};
