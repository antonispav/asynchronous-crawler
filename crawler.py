import asyncio
import logging
import aiohttp
from contextlib import closing
from urllib.parse import urljoin, urlparse
from lxml import html as lh
import time
import pandas as pd
import re
import requests


class AsyncCrawler:

    def __init__(self, start_url, max_concurrency=200):
        # max_concurrency : prevents our crawler from making more than 200 concurrent requests at a single time
        self.start_url = start_url
        self.base_url = '{}://{}'.format(urlparse(self.start_url).scheme, urlparse(self.start_url).netloc)
        self.seen_urls = set()
        self.external_urls = set()
        self.status = set()
        self.emails = set()
        self.failed = set()
        self.dfresults = pd.DataFrame(columns=['ParentUrl','Url','Text'])
        self.session = aiohttp.ClientSession() # this will throw a warning, but the creation of a client session is synchronous, so it can be safely done outside of a co-routine
        self.bounde_sempahore = asyncio.BoundedSemaphore(max_concurrency) # use this to prevent our crawler from making too many concurrent requests at one time

    async def _http_request(self, url, text):
        # get the response status code of page and the html content
        async with self.bounde_sempahore:
            try:
                async with self.session.get(url, timeout=30) as response:
                    return url, await response.read(), response.status, text
            except Exception as e:
                logging.warning('Request Exception For {} : {}'.format(url,e))
                # try:
                #     response2 = requests.get(url)
                #     return url, response2.content, response2.status_code, text
                # except Exception as ex:
                #     logging.warning('2 Request Exception For {} : {}'.format(url,ex))
                return url or ' ', ' ', ' ', ' '

    def find_urls(self, start_url, html):
        # get all the href & the correspondant text of a page. It will also collect the emails
        found_urls = []
        dom = lh.fromstring(html)
        for tag in dom.xpath('//a'):
            try:
                if 'href' not in tag.attrib: continue # the a tag has no href
                text = ' '
                if tag.text_content(): # get the a tag text
                    text = re.sub(' +',' ',tag.text_content().strip())# replace more than 1 space/tab with a singe space

                href = tag.attrib['href'].strip()
            except Exception as e:
                logging.warning('Urls Exception: {}'.format(e))
            else:
                if '@' in href: # found an email
                    self.emails.add((start_url,href,text))
                    continue
                if href.lower().startswith("javascript:"):
                    # found a javascript reference
                    continue
                url = urljoin(self.base_url, href)# relative url to absolute
                if url not in self.seen_urls:
                    found_urls.append((url, text))
        return start_url, found_urls

    async def extract_async(self, url, data):
        found_urls = []

        start_url, urls = self.find_urls(url, data)
        for url in urls:
            found_urls.append(url)
        return start_url, found_urls

    async def extract_multi_async(self, to_fetch):
        futures, results, wut, rwut = [], [], [], []
        #start = time.time()
        for url, text in to_fetch:
            if url or url != ' ':
                wut.append(self._http_request(url, text))
        for wu in asyncio.as_completed(wut):
            try:
                rwut.append((await wu))
            except Exception as e:
                logging.warning('Encountered Request exception: {}'.format(e))
        for url, data, status, text in rwut:
            if data and ((status == 200) or (status == 301)): # status Passed
                if url in self.seen_urls: continue
                self.seen_urls.add(url)
                if not url.startswith(self.base_url):
                    self.external_urls.add((url,text))
                    continue# external url, don't extract urls
                else:
                    futures.append(self.extract_async(url, data))
            else:
                self.failed.add((url,text))
                continue
            self.status.add((url,status,text))

        for future in asyncio.as_completed(futures):
            try:
                results.append((await future))
                #print(results)
            except Exception as e:
                logging.warning('Encountered exception: {}'.format(e))
        return results

    def parser(self, data):
        raise NotImplementedError

    async def crawl_async(self):
        to_fetch = [(self.start_url, ' ')]
        dfAll = pd.DataFrame(columns=['Url','Status','Text','ParentUrl'])
        dfFailedAll = pd.DataFrame()
        while True:

            if len(to_fetch) < 1: break
            batch = await self.extract_multi_async(to_fetch)
            to_fetch = []
            for start_url, found_urls in batch:
                merged, failed = self.ReadableData(found_urls,start_url,self.failed,self.external_urls,self.status)
                to_fetch.extend(set(found_urls))
        await self.session.close()
        return merged, failed, self.emails, self.external_urls, self.status



class ReadableData(AsyncCrawler):

    def ReadableData(self,found_urls,start_url,failed,external,status):
        dffailed = pd.DataFrame.from_records(list(failed), columns=['Url','Text'])
        dfstatus = pd.DataFrame.from_records(list(status), columns=['Url','Status','Text'])
        for url,text in found_urls:
            self.dfresults = self.dfresults.append({'ParentUrl': start_url, 'Url': url, 'Text': text}, ignore_index=True)

        dfMerged = dfstatus.merge(self.dfresults, how='left', left_on=['Url','Text'], right_on=['Url','Text'])
        dffailed = dffailed.merge(self.dfresults, how='left', left_on=['Url'], right_on=['Url'])
        return dfMerged, dffailed

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Link Validation Tool with Python")
    parser.add_argument("url", help="The URL to extract links from.")

    args = parser.parse_args()
    url = args.url
    domain_name = urlparse(url).netloc

    crawler = ReadableData(url)
    start = time.time()
    future = asyncio.Task(crawler.crawl_async())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(future)
    loop.close()
    results, failed, emails, external, status = future.result()

    dfexternal = pd.DataFrame.from_records(list(external), columns=['Url','Text'])
    emails = pd.DataFrame.from_records(list(emails), columns=['Url','Email','Text'])
    results = results.drop_duplicates()
    failed = failed.drop(['Text_x'], axis=1).rename(columns={'Text_y':'Text'}).drop_duplicates()

    print ("\n\n Final Results( you can find them in `results.csv`): \n",results)
    print('\n\n Failed Urls( you can find them in `failed.csv` ): \n',failed)
    print('Lenght of healthy Urls : ',len(status),'\n Lenght of broken Urls : ',len(failed))

    dfexternal.to_csv(f"{domain_name}_external_links.csv", sep='\t', index=False)
    emails.to_csv(f"{domain_name}_emails.csv", sep='\t', index=False)
    failed.to_csv(f"{domain_name}_failed_links.csv", sep='\t', index=False)
    results.to_csv(f"{domain_name}_results.csv", sep='\t', index=False)
