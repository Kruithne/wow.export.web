<?php
	if (php_sapi_name() == "cli")
	{
		file_put_contents(__DIR__ . "/listfile/master", fopen("https://raw.githubusercontent.com/wowdev/wow-listfile/master/community-listfile.csv", 'r'));
	}
	else
	{
		http_response_code(403);
	}