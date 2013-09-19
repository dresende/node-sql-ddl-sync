var common  = require("../common");
var Dialect = require("../../lib/Dialects/postgresql");

var should  = require("should");

describe("PostgreSQL.getType", function () {
	it("should detect text", function (done) {
		Dialect.getType({ type: "text" }).should.equal("VARCHAR(255)");
		Dialect.getType({ type: "text", size: 150 }).should.equal("VARCHAR(150)");
		Dialect.getType({ type: "text", size: 1000 }).should.equal("VARCHAR(1000)");

		return done();
	});

	it("should detect numbers", function (done) {
		Dialect.getType({ type: "number" }).should.equal("INTEGER");
		Dialect.getType({ type: "number", size: 4 }).should.equal("INTEGER");
		Dialect.getType({ type: "number", size: 2 }).should.equal("SMALLINT");
		Dialect.getType({ type: "number", size: 8 }).should.equal("BIGINT");

		return done();
	});

	it("should detect rational numbers", function (done) {
		Dialect.getType({ type: "number", rational: true }).should.equal("REAL");
		Dialect.getType({ type: "number", rational: true, size: 4 }).should.equal("REAL");
		Dialect.getType({ type: "number", rational: true, size: 8 }).should.equal("DOUBLE PRECISION");

		return done();
	});

	it("should detect booleans", function (done) {
		Dialect.getType({ type: "boolean" }).should.equal("BOOLEAN");

		return done();
	});

	it("should detect dates", function (done) {
		Dialect.getType({ type: "date" }).should.equal("DATE");

		return done();
	});

	it("should detect dates with times", function (done) {
		Dialect.getType({ type: "date", time: true }).should.equal("TIMESTAMP WITHOUT TIME ZONE");

		return done();
	});

	it("should detect binary", function (done) {
		Dialect.getType({ type: "binary" }).should.equal("BYTEA");

		return done();
	});

	it("should detect required items", function (done) {
		Dialect.getType({ type: "boolean", required: true }).should.match(/NOT NULL/);

		return done();
	});

	it("should detect default values", function (done) {
		Dialect.getType({ type: "number", defaultValue: 3 }).should.match(/DEFAULT 3/);

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
