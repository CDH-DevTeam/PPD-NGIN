if (window.console) {


	console.log("Welcome to your Play application's JavaScript!");

	http_req = new XMLHttpRequest();

	http_req.onreadystatechange = function() {
		if (http_req.readyState === XMLHttpRequest.DONE) {
			if (http_req.status === 200) {
				console.log(JSON.parse(http_req.responseText));
				//console.log(http_req.responseText);
			} else {
				console.log(http_req.responseText);
			}
		}
	}

	http_req.open('GET', 'http://0.0.0.0:9000/motioner?tags=hej,test');
	http_req.send();

}