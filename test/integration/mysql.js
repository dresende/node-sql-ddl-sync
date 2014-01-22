var should  = require("should");
var common  = require("../common");
var Dialect = require("../../lib/Dialects/mysql");
var driver  = common.fakeDriver;

describe("MySQL.getType", function () {
	it("should detect text", function (done) {
		Dialect.getType(null, null, { type: "text" }, driver).value.should.equal("VARCHAR(255)");
		Dialect.getType(null, null, { type: "text", size: 150 }, driver).value.should.equal("VARCHAR(150)");
		Dialect.getType(null, null, { type: "text", size: 1000 }, driver).value.should.equal("VARCHAR(1000)");

		return done();
	});

	it("should detect numbers", function (done) {
		Dialect.getType(null, null, { type: "number" }, driver).value.should.equal("INTEGER");
		Dialect.getType(null, null, { type: "number", size: 4 }, driver).value.should.equal("INTEGER");
		Dialect.getType(null, null, { type: "number", size: 2 }, driver).value.should.equal("SMALLINT");
		Dialect.getType(null, null, { type: "number", size: 8 }, driver).value.should.equal("BIGINT");

		return done();
	});

	it("should detect rational numbers", function (done) {
		Dialect.getType(null, null, { type: "number", rational: true }, driver).value.should.equal("FLOAT");
		Dialect.getType(null, null, { type: "number", rational: true, size: 4 }, driver).value.should.equal("FLOAT");
		Dialect.getType(null, null, { type: "number", rational: true, size: 8 }, driver).value.should.equal("DOUBLE");

		return done();
	});

	it("should detect booleans", function (done) {
		Dialect.getType(null, null, { type: "boolean" }, driver).value.should.equal("TINYINT(1)");

		return done();
	});

	it("should detect dates", function (done) {
		Dialect.getType(null, null, { type: "date" }, driver).value.should.equal("DATE");

		return done();
	});

	it("should detect dates with times", function (done) {
		Dialect.getType(null, null, { type: "date", time: true }, driver).value.should.equal("DATETIME");

		return done();
	});

	it("should detect binary", function (done) {
		Dialect.getType(null, null, { type: "binary" }, driver).value.should.equal("BLOB");

		return done();
	});

	it("should detect big binary", function (done) {
		Dialect.getType(null, null, { type: "binary", big: true }, driver).value.should.equal("LONGBLOB");

		return done();
	});

	it("should detect required items", function (done) {
		Dialect.getType(null, null, { type: "boolean", required: true }, driver).value.should.match(/NOT NULL/);

		return done();
	});

	it("should detect default values", function (done) {
		Dialect.getType(null, null, { type: "number", defaultValue: 3 }, driver).value.should.match(/DEFAULT \^\^3\^\^/);

		return done();
	});

	it("should detect serial", function (done) {
		var column = Dialect.getType(null, null, {
			type   : "number",
			serial : true
		}).value;

		column.should.match(/INT/);
		column.should.match(/NOT NULL/);
		column.should.match(/AUTO_INCREMENT/);

		return done();
	});
});
