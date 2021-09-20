# Python Urls Verification

An asynchronous python script with asyncio to validate all urls in a website. A url fails when the returned status code is not 200(aiohttp can't follow permanent redirects with status code : 301)

#### Usage :
* Help : ```python3 crawler.py --help```
* Run Example : ```python3 crawler.py https://site.com/```

#### Output :
* ```_emails.csv ``` :
    > All the emails found in the website
    > Columns :
    > * ```Url``` : In which page the email was found
    > * ```Email``` : The the email address
    > * ```Text``` : The appearing text of the Email
* ```_external_links.txt``` :
    > All the links referring to external websites
    > Columns :
    > * ```Url``` : In which page the link was found
    > * ```Text``` : The appearing text of the href
* ```_failed_links.csv``` :
    > All the links that returned a status code different that 200
    > Columns :
    > * ```Url``` : The link that returned a status code different that 200
    > * ```ParentUrl``` : In which page the broken link was found
    > * ```Text``` : The appearing text of the href
* ```_results.csv``` :
    > All the links that returned 200 status code
    > Columns :
    > * ```Url``` : The link that returned 200 status code
    > * ```Status``` : Status code of url
    > * ```Text``` : The appearing text of the href
    > * ```ParentUrl``` : In which page the link was found
