exports.fakeDialect = {
	escapeId : function (id) {
		return "$$" + id + "$$";
	}
};
