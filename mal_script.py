import aiohttp
from urllib.parse import urlunparse, urlencode
import asyncio

class AsyncAPIClient:
    def __init__(self, base_url:str, headers : dict = {}):
        self.base_url = base_url.rstrip('/')
        self.headers = headers
    
    async def __aenter__(self):
        # connector = aiohttp.TCPConnector(limit=1)  # 1 concurrent connection
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def generate_url(self, path='', query_dict={}):
        scheme = 'https'
        netloc = self.base_url

        new_query = urlencode(query_dict, doseq=True)

        new_url = urlunparse((scheme, netloc, path, '', new_query, ''))
        return new_url
    
    async def _make_request(self,method, endpoint:str, params : dict):
        url = self.generate_url(path=endpoint, query_dict=params)
        print(url)
        try:
            async with self.session.request(method,url) as response:
                response.raise_for_status()
                return response
        except aiohttp.ClientError as e:
            print(f"request failed : {e}")
            raise e
    
    async def getv2(self, path: str, params: dict = {}):
        api_url = self.generate_url(path, params)
        async with self.session.get(api_url) as res:
            return await res.json()

async def main():
    async with AsyncAPIClient("api.jikan.moe/v4",headers={}) as client:
        semaphore = asyncio.Semaphore(1)

        async def get_with_semaphore(endpoint, params):
            async with semaphore:
                return await client.getv2(endpoint,params)
        task1 = get_with_semaphore('seasons/now', params={'page':1})
        task2 = get_with_semaphore('seasons/now', params={'page':2})
        tasks = []

        tasks.append(task1)
        tasks.append(task2)
        # results = asyncio.gather(*tasks, return_exceptions=True)

        results = await asyncio.gather(*tasks,return_exceptions=True)
        for i, result in enumerate(results):    
            if isinstance(result, Exception):
                print(f"Task {i+1} failed: {result}")
            else:
                # Extract just the 'pagination' dict from the response
                pagination = result.get('pagination', {})  # type: ignore # Use .get() to avoid KeyError if missing
                print(f"Task {i+1} pagination: {pagination}")



if __name__ == "__main__":
    asyncio.run(main())
