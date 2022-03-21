<?php
	if (isset($_GET['def'])) {
		header('Content-Type: text/plain');
		echo file_get_contents(sprintf('https://raw.githubusercontent.com/wowdev/WoWDBDefs/master/definitions/%s.dbd', $_GET['def']));
	} else {
		http_response_code(404);
	}