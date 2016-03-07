if (window.console) {


	console.log("Welcome to your Play application's JavaScript!");

	http_req = new XMLHttpRequest();

	http_req.onreadystatechange = function() {
		if (http_req.readyState === XMLHttpRequest.DONE) {
			if (http_req.status === 200) {
				//console.log(JSON.parse(http_req.responseText));
				console.log(http_req.responseText);
			} else {
				console.log(http_req.responseText);
			}
		}
	}

	test_terms = [
		'asdf',
		'asdf,qwer,zxcv parti:(m,s) år:(1995-2000)',
		'mer .* till författare:(anders borg)',
		'mer pengar till .*'
	]

	test_terms.forEach(function(entry) {
		http_req.open('GET', 'http://0.0.0.0:9000/motioner?search_phrase=' + entry, false);
		http_req.send();
	});

}