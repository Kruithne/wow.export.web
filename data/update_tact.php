<?php
	if (php_sapi_name() == "cli")
	{
		file_put_contents(__DIR__ . "/tact/wow", fopen("https://raw.githubusercontent.com/wowdev/TACTKeys/master/WoW.txt", 'r'));
	}
	else
	{
		http_response_code(403);
	}