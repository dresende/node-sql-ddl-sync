var common  = require("../common");
var Dialect = require("../../lib/Dialects/mysql");

var should  = require("should");

describe("MySQL.getType", function () {
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
		Dialect.getType({ type: "number", rational: true }).should.equal("FLOAT");
		Dialect.getType({ type: "number", rational: true, size: 4 }).should.equal("FLOAT");
		Dialect.getType({ type: "number", rational: true, size: 8 }).should.equal("DOUBLE");

		return done();
	});

	it("should detect booleans", function (done) {
		Dialect.getType({ type: "boolean" }).should.equal("TINYINT(1)");

		return done();
	});

	it("should detect dates", function (done) {
		Dialect.getType({ type: "date" }).should.equal("DATE");

		return done();
	});

	it("should detect dates with times", function (done) {
		Dialect.getType({ type: "date", time: true }).should.equal("DATETIME");

		return done();
	});

	it("should detect binary", function (done) {
		Dialect.getType({ type: "binary" }).should.equal("BLOB");

		return done();
	});

	it("should detect big binary", function (done) {
		Dialect.getType({ type: "binary", big: true }).should.equal("LONGBLOB");

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

	it("should detect serial", function (done) {
		var column = Dialect.getType({
			type   : "number",
			serial : true
		});

		column.should.match(/INT/);
		column.should.match(/NOT NULL/);
		column.should.match(/AUTO_INCREMENT/);

		return done();
	});
});

describe("MySQL.escapeId", function () {
	it("should correctly escape identifiers", function (done) {
		Dialect.escapeId("my_id").should.equal("`my_id`");
		Dialect.escapeId("my_`id").should.equal("`my_``id`");
		Dialect.escapeId("my_id", "sub").should.equal("`my_id`.`sub`");

		return done();
	});
});
