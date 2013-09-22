var program = require("commander");
var Mocha   = require("mocha");
var url     = require("url");

var common  = require("./common");

program.version("0.1.0")
       .option("-u, --uri <uri>", "Database URI", String, null)
       .parse(process.argv);

if (!program.uri) {
	program.outputHelp();
	process.exit(1);
}

var uri = url.parse(program.uri);

if (!uri.hasOwnProperty("protocol") || !uri.protocol) {
	program.outputHelp();
	process.exit(1);
}

switch (uri.protocol) {
	case "mysql:":
		common.dialect = "mysql";

		common.db = require("mysql").createConnection(program.uri);
		common.db.connect(testDatabase);
		break;
	case "postgres:":
	case "postgresql:":
	case "pg:":
		common.dialect = "postgresql";

		common.db = new (require("pg").Client)(uri);
		common.db.connect(testDatabase);
		break;
	case "sqlite:":
	case "sqlite3:":
		common.dialect = "sqlite";

		common.db = new (require("sqlite3").Database)(uri.pathname);
		testDatabase();
		// common.db.connect(testDatabase);
		break;
	default:
		process.stdout.write("Database protocol not supported.\n");
		process.exit(2);
}

function testDatabase(err) {
	if (err) {
		throw err;
	}

	var mocha    = new Mocha({
		reporter : "spec"
	});
	mocha.addFile(__dirname + "/integration/db.js");
	mocha.run(function (failures) {
		process.exit(failures);
	});
}
