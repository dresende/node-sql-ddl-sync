var common = require("./common");
var Mocha  = require("mocha");
var path   = require("path");
var fs     = require("fs");

var location = __dirname + "/integration/";
var mocha    = new Mocha({
	reporter : "progress"
});

fs.readdirSync(location).filter(function (file) {
	return file.substr(-3) === '.js';
}).forEach(function (file) {
	mocha.addFile(
		path.join(location, file)
	);
});

mocha.run(function (failures) {
	process.exit(failures);
});
