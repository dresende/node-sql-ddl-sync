var common  = require("../common");
var Dialect = require("../../lib/Dialects/postgresql");

var should  = require("should");

describe("PostgreSQL.getType", function () {
	it("should detect text", function (done) {
		Dialect.getType(null, null, { type: "text" }).value.should.equal("TEXT");
		Dialect.getType(null, null, { type: "text", size: 150 }).value.should.equal("TEXT");
		Dialect.getType(null, null, { type: "text", size: 1000 }).value.should.equal("TEXT");

		return done();
	});

	it("should detect numbers", function (done) {
		Dialect.getType(null, null, { type: "number" }).value.should.equal("INTEGER");
		Dialect.getType(null, null, { type: "number", size: 4 }).value.should.equal("INTEGER");
		Dialect.getType(null, null, { type: "number", size: 2 }).value.should.equal("SMALLINT");
		Dialect.getType(null, null, { type: "number", size: 8 }).value.should.equal("BIGINT");

		return done();
	});

	it("should detect rational numbers", function (done) {
		Dialect.getType(null, null, { type: "number", rational: true }).value.should.equal("REAL");
		Dialect.getType(null, null, { type: "number", rational: true, size: 4 }).value.should.equal("REAL");
		Dialect.getType(null, null, { type: "number", rational: true, size: 8 }).value.should.equal("DOUBLE PRECISION");

		return done();
	});

	it("should detect booleans", function (done) {
		Dialect.getType(null, null, { type: "boolean" }).value.should.equal("BOOLEAN");

		return done();
	});

	it("should detect dates", function (done) {
		Dialect.getType(null, null, { type: "date" }).value.should.equal("DATE");

		return done();
	});

	it("should detect dates with times", function (done) {
		Dialect.getType(null, null, { type: "date", time: true }).value.should.equal("TIMESTAMP WITHOUT TIME ZONE");

		return done();
	});

	it("should detect binary", function (done) {
		Dialect.getType(null, null, { type: "binary" }).value.should.equal("BYTEA");

		return done();
	});

	it("should detect required items", function (done) {
		Dialect.getType(null, null, { type: "boolean", required: true }).value.should.match(/NOT NULL/);

		return done();
	});

	it("should detect default values", function (done) {
		Dialect.getType(null, null, { type: "number", defaultValue: 3 }).value.should.match(/DEFAULT 3/);

		return done();
	});
});

describe("PostgreSQL.escapeId", function () {
	it("should correctly escape identifiers", function (done) {
		Dialect.escapeId("my_id").should.equal('"my_id"');
		Dialect.escapeId("my_\"id").should.equal('"my_""id"');
		Dialect.escapeId("my_id", "sub").should.equal('"my_id"."sub"');

		return done();
	});
});
