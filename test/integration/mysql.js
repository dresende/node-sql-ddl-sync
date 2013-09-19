var common  = require("../common");
var Dialect = require("../../lib/Dialects/mysql");

var should  = require("should");

describe("MySQL.getType", function () {
	it("should detect text", function (done) {
		Dialect.getType({ type: "text" }).should.equal("VARCHAR(255)");
		Dialect.getType({ type: "text", size: 150 }).should.equal("VARCHAR(150)");

		return done();
	});

	it("should detect numbers", function (done) {
		Dialect.getType({ type: "number" }).should.equal("INT(5)");

		return done();
	});

	it("should detect rational numbers", function (done) {
		Dialect.getType({ type: "number", rational: true }).should.equal("FLOAT");

		return done();
	});

	it("should detect booleans", function (done) {
		Dialect.getType({ type: "boolean" }).should.equal("TINYINT(1)");

		return done();
	});

	it("should detect required items", function (done) {
		Dialect.getType({ type: "boolean", required: true }).should.match(/NOT NULL/);

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
