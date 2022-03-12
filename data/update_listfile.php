<?php
	if (php_sapi_name() == "cli")
	{
		file_put_contents(__DIR__ . "/listfile/master.tmp", fopen("https://raw.githubusercontent.com/wowdev/wow-listfile/master/community-listfile.csv", 'r'));
		rename(__DIR__ . "/listfile/master.tmp", __DIR__ . "/listfile/master");
	}
	else
	{
		http_response_code(403);
	}