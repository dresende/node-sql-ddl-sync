## NodeJS SQL DDL Synchronization

[![Build Status](https://secure.travis-ci.org/dresende/node-sql-ddl-sync.png?branch=master)](http://travis-ci.org/dresende/node-sql-ddl-sync)

## Install

```sh
npm install sql-ddl-sync
```

## Dialects

- MySQL

## About

This module is used by [ORM](http://dresende.github.com/node-orm2) to synchronize model tables in the different supported
dialects. Sorry there is no API documentation for now but there are a couple of tests you can read and find out how to use
it if you want.

## Example

Install module and install `mysql`, create a file with the contents below and change line 2 to match valid credentials.
Run once and you'll see table `ddl_sync_test` appear in your database. Then make some changes to it (add/drop/change columns)
and run the code again. Your table should always return to the same structure.

```js
var mysql = require("mysql");
var db    = mysql.createConnection("mysql://username:password@localhost/database");

var Sync = require("./lib/Sync").Sync;
var sync = new Sync({
	dialect : "mysql",
	db      : db,
	debug   : function (text) {
		console.log("> %s", text);
	}
});

sync.define("ddl_sync_test", {
	id     : { type : "number", primary: true, serial: true },
	name   : { type : "text", required: true },
	age    : { type : "number", rational: true },
	male   : { type : "boolean" },
	born   : { type : "date", time: true },
	born2  : { type : "date" },
	int2   : { type : "number", size: 2 },
	int4   : { type : "number", size: 4 },
	int8   : { type : "number", size: 8 },
	float4 : { type : "number", rational: true, size: 4 },
	float8 : { type : "number", rational: true, size: 8 },
	type   : { type : "enum", values: [ 'dog', 'cat'], defaultValue: 'dog', required: true },
	photo  : { type : "binary" }
});

sync.sync(function (err) {
	if (err) {
		console.log("> Sync Error");
		console.log(err);
	} else {
		console.log("> Sync Done");
	}
	process.exit(0);
});

```
