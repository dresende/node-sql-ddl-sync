var should  = require("should");
var sinon   = require("sinon");
var common  = require("../common");
var Dialect = require("../../lib/Dialects/postgresql");
var driver  = common.fakeDriver;

describe("PostgreSQL.getCollectionProperties", function () {
	it("should detect UUID", function () {
		should.deepEqual(
			Dialect.getColumnProperties({ data_type: 'uuid' }),
			{ type: 'uuid' }
		);
	});
});

describe("PostgreSQL.getType", function () {
	it("should detect text", function () {
		Dialect.getType(null, { mapsTo: 'abc',  type: "text" }, driver).value.should.equal("TEXT");
		Dialect.getType(null, { mapsTo: 'abc',  type: "text", size: 150 }, driver).value.should.equal("TEXT");
		Dialect.getType(null, { mapsTo: 'abc',  type: "text", size: 1000 }, driver).value.should.equal("TEXT");
	});

	it("should detect numbers", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "integer" }, driver).value.should.equal("INTEGER");
		Dialect.getType(null, { mapsTo: 'abc', type: "integer", size: 4 }, driver).value.should.equal("INTEGER");
		Dialect.getType(null, { mapsTo: 'abc', type: "integer", size: 2 }, driver).value.should.equal("SMALLINT");
		Dialect.getType(null, { mapsTo: 'abc', type: "integer", size: 8 }, driver).value.should.equal("BIGINT");
		Dialect.getType(null, { mapsTo: 'abc', type: "number", rational: false }, driver).value.should.equal("INTEGER");
	});

	it("should detect rational numbers", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "number"}, driver).value.should.equal("REAL");
		Dialect.getType(null, { mapsTo: 'abc', type: "number", size: 4 }, driver).value.should.equal("REAL");
		Dialect.getType(null, { mapsTo: 'abc', type: "number", size: 8 }, driver).value.should.equal("DOUBLE PRECISION");
	});

	it("should detect booleans", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "boolean" }, driver).value.should.equal("BOOLEAN");
	});

	it("should detect dates", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "date" }, driver).value.should.equal("DATE");
	});

	it("should detect dates with times", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "date", time: true }, driver).value.should.equal("TIMESTAMP WITHOUT TIME ZONE");
	});

	it("should detect binary", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "binary" }, driver).value.should.equal("BYTEA");
	});

	it("should detect uuid", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "uuid" }, driver).value.should.equal("UUID");
	});

	it("should detect custom types", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "json" }, driver).value.should.equal("JSON");
	});

	it("should detect required items", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "boolean", required: true }, driver).value.should.match(/NOT NULL/);
	});

	it("should detect default values", function () {
		Dialect.getType(null, { mapsTo: 'abc', type: "number", defaultValue: 3 }, driver).value.should.match(/DEFAULT \^\^3\^\^/);
		Dialect.getType(null, { mapsTo: 'abc', type: 'date',   defaultValue: Date.now }, driver).value.should.equal('DATE DEFAULT now()');
	});

	it("should detect default expressions", function () {
		should.equal(
			Dialect.getType(null, { mapsTo: 'abc', type: 'uuid', defaultExpression: 'uuid_generate_v4()' }, driver).value,
			'UUID DEFAULT uuid_generate_v4()'
		);
	});
});
