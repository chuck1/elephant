
class ElephantFilterForm
{
	constructor(callback) {

		this.div = $("<div>");

		var input = $('<input type="text">');

		this.div.append(input);

		var button = $("<button>");

		button.click((ev) => {
			callback(input.val());
		})

		this.div.append(button);
	}
}

var elephant = {

}

