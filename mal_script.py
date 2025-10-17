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
    
    async def getv2(self, path:str, params : dict = {}):
        async with self.session as session:
            api_url = self.generate_url(path,params)
            async with session.get(api_url) as res:
                return await res.json()
        
    # async def get(self, endpoint:str, params={}):
    #     async with self._make_request('GET',endpoint,parasm):
    #     response = await self._make_request('GET', endpoint, params)
        
        # content_type = response.headers.get('content-type', '').split(';')[0].strip()
        # if content_type == 'application/json':
        #     print("masuk sini")
        #     data =  await response.json()
        #     # print(data)
        #     return data
        # else:
        #     return await response.text()

async def main():
    async with AsyncAPIClient("api.jikan.moe/v4",headers={}) as client:
        tasks = []
        task1 = client.getv2('seasons/now', params={'page':1})
        task2 = client.getv2('seasons/now', params={'page':2})
        task3 = client.getv2('seasons/now', params={'page':3})
        
        tasks.append(task1)
        tasks.append(task2)
        tasks.append(task3)
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


    # async def main():
#     async with AsyncAPIClient("api.jikan.moe/v4", headers={}) as client:
#         semaphore = asyncio.Semaphore(1)  # Or 2 for slight parallelism

#         async def get_with_semaphore(endpoint, params):
#             async with semaphore:
#                 return await client.get(endpoint, params)

#         task1 = get_with_semaphore('seasons/now', params={'page': 1})
#         task2 = get_with_semaphore('seasons/now', params={'page': 2})
#         tasks = [task1, task2]

#         results = await asyncio.gather(*tasks, return_exceptions=True)

#         for i, result in enumerate(results):
#             if isinstance(result, Exception):
#                 print(f"Task {i+1} failed: {result}")
#             else:
#                 print(f"Task {i+1} result: {result}")