var Sync    = require("../../lib/Sync").Sync;
var common  = require("../common");
var should  = require("should");
var sync    = new Sync({
	dialect : common.dialect,
	db      : common.db,
	debug   : function (text) {
		// console.log("> %s", text);
	}
});

sync.defineCollection(common.table, {
	id     : { type: "number", primary: true, serial: true },
	name   : { type: "text", required: true, defaultValue: 'John' },
	age    : { type: "number", rational: true },
	male   : { type: "boolean" },
	dttm   : { type: "date", time: true, index: true },
	dt     : { type: "date" },
	int2   : { type: "number", size: 2, index: [ "idx1", "idx2" ] },
	int4   : { type: "number", size: 4 },
	int8   : { type: "number", size: 8, index: [ "idx2" ] },
	float4 : { type: "number", rational: true, size: 4 },
	float8 : { type: "number", rational: true, size: 8 },
	enm    : { type: "enum", values: [ 'dog', 'cat'], defaultValue: 'dog', required: true },
	binry  : { type: "binary" }
});

describe("Synching", function () {
	it("should create the table", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes");

			return done();
		});
	});

	it("should have no changes on second call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 0);

			return done();
		});
	});
});

if (common.dialect != "sqlite") {
	describe("Dropping a column", function () {
		before(common.dropColumn('dt'));

		it("should recreate it on first call", function (done) {
			sync.sync(function (err, info) {
				should.not.exist(err);
				should.exist(info);
				info.should.have.property("changes", 1);

				return done();
			});
		});

		it("should have no changes on second call", function (done) {
			sync.sync(function (err, info) {
				should.not.exist(err);
				should.exist(info);
				info.should.have.property("changes", 0);

				return done();
			});
		});
	});

	describe("Dropping a column that has an index", function () {
		before(common.dropColumn('dttm'));

		it("should recreate column and index on first call", function (done) {
			sync.sync(function (err, info) {
				should.not.exist(err);
				should.exist(info);
				info.should.have.property("changes", 2);

				return done();
			});
		});

		it("should have no changes on second call", function (done) {
			sync.sync(function (err, info) {
				should.not.exist(err);
				should.exist(info);
				info.should.have.property("changes", 0);

				return done();
			});
		});
	});

	describe("Adding a column", function () {
		before(common.addColumn('unknown_col'));

		it("should drop column on first call", function (done) {
			sync.sync(function (err, info) {
				should.not.exist(err);
				should.exist(info);
				info.should.have.property("changes", 1);

				return done();
			});
		});

		it("should have no changes on second call", function (done) {
			sync.sync(function (err, info) {
				should.not.exist(err);
				should.exist(info);
				info.should.have.property("changes", 0);

				return done();
			});
		});
	});
}

describe("Changing a column", function () {
	before(common.changeColumn('int4'));

	it("should update column on first call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 1);

			return done();
		});
	});

	it("should have no changes on second call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 0);

			return done();
		});
	});
});

describe("Adding an index", function () {
	before(common.addIndex('xpto', 'dt'));

	it("should drop index on first call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 1);

			return done();
		});
	});

	it("should have no changes on second call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 0);

			return done();
		});
	});
});

describe("Dropping an index", function () {
	before(common.dropIndex('idx2'));

	it("should drop index on first call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 1);

			return done();
		});
	});

	it("should have no changes on second call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 0);

			return done();
		});
	});
});

describe("Changing index to unique index", function () {
	before(function (done) {
		common.dropIndex('dttm_index')(function () {
			common.addIndex('dttm_index', 'dttm', true)(done);
		});
	});

	it("should drop index and recreate it on first call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 1);

			return done();
		});
	});

	it("should have no changes on second call", function (done) {
		sync.sync(function (err, info) {
			should.not.exist(err);
			should.exist(info);
			info.should.have.property("changes", 0);

			return done();
		});
	});
});
