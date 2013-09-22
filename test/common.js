exports.dialect = null;
exports.table   = "sql_ddl_sync_test_table";

exports.fakeDialect = {
	escapeId : function (id) {
		return "$$" + id + "$$";
	}
};

exports.dropColumn = function (column) {
	return function (done) {
		switch (exports.dialect) {
			case "mysql":
				return exports.db.query("ALTER TABLE ?? DROP ??", [ exports.table, column ], done);
			case "postgresql":
				return exports.db.query("ALTER TABLE " + exports.table + " DROP " + column, done);
		}
		return done(unknownProtocol());
	};
};

exports.addColumn = function (column) {
	return function (done) {
		switch (exports.dialect) {
			case "mysql":
				return exports.db.query("ALTER TABLE ?? ADD ?? INTEGER NOT NULL", [ exports.table, column ], done);
			case "postgresql":
				return exports.db.query("ALTER TABLE " + exports.table + " ADD " + column + " INTEGER NOT NULL", done);
			case "sqlite":
				return exports.db.all("ALTER TABLE " + exports.table + " ADD " + column + " INTEGER", done);
		}
		return done(unknownProtocol());
	};
};

exports.changeColumn = function (column) {
	return function (done) {
		switch (exports.dialect) {
			case "mysql":
				return exports.db.query("ALTER TABLE ?? MODIFY ?? INTEGER NOT NULL", [ exports.table, column ], done);
			case "postgresql":
				return exports.db.query("ALTER TABLE " + exports.table + " ALTER " + column + " TYPE DOUBLE PRECISION", done);
			case "sqlite":
				return exports.db.all("ALTER TABLE " + exports.table + " MODIFY " + column + " INTEGER NOT NULL", done);
		}
		return done(unknownProtocol());
	};
};

exports.addIndex = function (name, column, unique) {
	return function (done) {
		switch (exports.dialect) {
			case "mysql":
				return exports.db.query("CREATE " + (unique ? "UNIQUE" : "") + " INDEX ?? ON ?? (??)", [ name, exports.table, column ], done);
			case "postgresql":
				return exports.db.query("CREATE " + (unique ? "UNIQUE" : "") + " INDEX " + exports.table + "_" + name + " ON " + exports.table + " (" + column + ")", done);
			case "sqlite":
				return exports.db.all("CREATE " + (unique ? "UNIQUE" : "") + " INDEX " + name + " ON " + exports.table + " (" + column + ")", done);
		}
		return done(unknownProtocol());
	};
};

exports.dropIndex = function (name) {
	return function (done) {
		switch (exports.dialect) {
			case "mysql":
				return exports.db.query("DROP INDEX ?? ON ??", [ name, exports.table ], done);
			case "postgresql":
				return exports.db.query("DROP INDEX " + exports.table + "_" + name, done);
			case "sqlite":
				return exports.db.all("DROP INDEX " + name, done);
		}
		return done(unknownProtocol());
	};
};

function unknownProtocol() {
	return new Error("Unknown protocol - " + exports.dialect);
}
