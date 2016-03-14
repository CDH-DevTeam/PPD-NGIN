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
	/*
	test_terms = [
		'pengar, krig parti:(m)',
		'pengar parti:(m), krig parti:(m)',
		'"pengar, krig" parti:(m)',
		'pengar parti:(s), pengar parti:(m)',
		'pengar parti:(s, v, mp), pengar parti:(m,fp,l,c)',
		'mer pengar till, krig år:(97-99)',
		'mer pengar till, krig år:(1997-1999)',
		'mer pengar till det här landet år:(1997-1999)',
		'mer pengar till .* år:(19970801-19990801)',
		'mer pengar till .* för att kunna',
		'mer pengar till landet för .* kunna',
		'mer .* till landet för att kunna',
		'mer .* till'
	]
	*/
	test_terms = [
		'skola, vård, mer pengar, mer utbildning'
	]

	test_terms.forEach(function(entry) {
		http_req.open('GET', 'http://0.0.0.0:9000/motioner?searchPhrase=' + entry, false);
		http_req.send();
	});

}