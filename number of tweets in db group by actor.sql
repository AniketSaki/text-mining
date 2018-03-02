SELECT
	verified, COUNT(*)
FROM
	actor
GROUP BY verified
ORDER BY 2 DESC;

SELECT
	location, COUNT(*)
FROM
	actor
GROUP BY location
ORDER BY 2 DESC;

SELECT
	timezone, COUNT(*)
FROM
	actor
GROUP BY timezone
ORDER BY 2 DESC;

SELECT
	utcOffset, COUNT(*)
FROM
	actor
GROUP BY utcOffset
ORDER BY 2 DESC;

SELECT 
    COUNT(*) - COUNT(href) AS 'Null', COUNT(href) AS 'Not Null'
FROM
    actor_links;

SELECT 
    idactor, COUNT(*)
FROM
    tweet
GROUP BY idactor
ORDER BY 2 DESC;

SELECT
	value, COUNT(*)
FROM
	gnip
GROUP BY value
ORDER BY 2 DESC;

SELECT
	idobject, COUNT(*)
FROM
	object
GROUP BY idobject
ORDER BY 2 DESC;

SELECT
	text, COUNT(*)
FROM
	hashtags
GROUP BY text
ORDER BY 2 DESC;

SELECT
	text, COUNT(*)
FROM
	symbols
GROUP BY text
ORDER BY 2 DESC;

SELECT
	url, COUNT(*)
FROM
	urls
GROUP BY url
ORDER BY 2 DESC;

SELECT
	screen_name, COUNT(*)
FROM
	mentions
GROUP BY screen_name
ORDER BY 2 DESC;

SELECT
	idmedia, COUNT(*)
FROM
	media
GROUP BY idmedia
ORDER BY 2 DESC;

SELECT
	location, COUNT(*)
FROM
	actor
GROUP BY location
ORDER BY 2 DESC;

SELECT
	displayName, country_code, twitter_country_code, COUNT(*)
FROM location
GROUP BY displayName
ORDER BY 4 DESC;

SELECT 
    verb, COUNT(*)
FROM
    tweet
GROUP BY verb
ORDER BY 2 DESC;

SELECT
	twitter_lang, COUNT(*)
FROM
	tweet
GROUP BY twitter_lang
ORDER BY 2 DESC;

SELECT
	generator_name, COUNT(*)
FROM tweet
GROUP BY generator_name
ORDER BY 2 DESC;

SELECT
	provider_name, COUNT(*)
FROM tweet
GROUP BY provider_name
ORDER BY 2 DESC;

