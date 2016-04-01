if (window.console) {


	console.log("Welcome to your Play application's JavaScript!");

	http_req = new XMLHttpRequest();

	http_req.onreadystatechange = function() {
		if (http_req.readyState === XMLHttpRequest.DONE) {
			if (http_req.status === 200) {
				var json_data = JSON.parse(http_req.responseText)
				console.log(json_data);
				//console.log(json_data[0]['hits'][0]['_source']['dokument']['html']);
				//console.log(http_req.responseText);
				
			} else {
				console.log(http_req.responseText);
			}
		}
	}

	//http_req.open('GET', 'http://0.0.0.0:9000/motioner/timeline/total', false);
	//http_req.send();

	/*
	test_terms = [
		'pengar, krig parti:(m)',
		'pengar parti:(m), krig parti:(m)',
		'(pengar, krig) parti:(m)',
		'(pengar, krig) parti:(m), (invandring, lönsamhet) parti:(s)',
		'pengar parti:(s), pengar parti:(m)',
		'pengar parti:(s, v, mp), pengar parti:(m,fp,l,c)',
		'mer pengar till * för att kunna',
		'mer pengar till landet för * kunna',
		'mer * till landet för att kunna',
		'* pengar till',
		'pengar parti:(s,v) titel:(ekonomi, pengar), mer pengar, mer pengar till *',
		'parti:(s), parti:(m)',
		''
	]
	*/
	
	test_terms = [
		'pengar, krig parti:(m)',
	]
	
	
	
	
	

	test_terms.forEach(function(entry) {
		//http_req.open('GET', 'http://0.0.0.0:9000/motioner/timeline/search?searchPhrase=' + entry, false);
		//http_req.open('GET', 'http://0.0.0.0:9000/motioner/hits?searchPhrase=' + entry + '&startDate=2006&endDate=2008&fromIndex=3000', false);
		http_req.open('GET', 'http://0.0.0.0:9000/queries/latest', false);
		http_req.send();
		http_req.open('GET', 'http://0.0.0.0:9000/queries/top', false);
		http_req.send();
	});
	
}