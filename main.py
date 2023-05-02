import asyncio, collections, bs4, json
import random
import time
import aiohttp
from aiocfscrape import CloudflareScraper
from bs4 import BeautifulSoup as bs
from aiohttp import ClientTimeout
from datetime import date
from pathlib import Path
from _io import TextIOWrapper

BASE_DOMAIN = 'https://online.globus.ru'
TIME_OUT = ClientTimeout(total=3600)
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
PROXIES = json.load(open('proxies.json'))

Product = collections.namedtuple('Product', 'date brand name pics price promo_price card_price rating desc comp url')


class GetTagValue:
    def __init__(self, tag: bs4.Tag):
        self.tag = tag

    def value(self, tag_name: str) -> str | None:
        try:
            return self.tag.find(tag_name).text
        except:
            return None

    def scope(self, tag_name: str, scope: dict):
        try:
            tag = self.tag.find(tag_name, scope)
            return tag.text
        except:
            return None

    # рефакторинг ценника без скидок
    def oldprice(self) -> str|None:
        try:
            oldprice_tag = self.tag.find('span', {'class': 'item-price__old'})
            oldprice_main = oldprice_tag.text.replace('\n', '').replace('\xa0', '').replace(' ', '')
            oldprice_sup = oldprice_tag.find('sup').text

            oldprice = oldprice_main[:-len(oldprice_sup)] + f'.{oldprice_sup}'
            return oldprice

        except Exception as e:
            return None


class GetDictValue:
    def __init__(self, dict):
        self.dict = dict

    def value(self, key):
        try:
            return self.dict[key]
        except:
            return None

def list_to_dict(arr):
    dict = {}
    for i in range(len(arr) + 1):
        try:
            if not i % 2:
                dict.update({arr[i]: arr[i + 1]})
        except IndexError:
            continue

    return dict


# функция итерирует объект Future и превращает его в список
# также отливливает ситуации, если одна из корутин не отработала и\или отаботала некорректно
# возвращает список со значениями из футуры или пустой список
def products_to_list(products):
    print('products to list working')
    products_list = []
    for arr in products:
        print(type(arr), len(arr))
        if not isinstance(arr, list):
            print('arr is not list')
            return []
        for product in arr:
            products_list.append(product)

    return products_list

def find_last_page_number(soup: bs4.Tag) -> int:
    li_tags = soup.find_all('li')
    top = 0
    for tag in li_tags:
        # try/except нужен для того, чтобы программа не наебнулась
        # если в итерируемом теге нет тега a и/или аттрибута href
        try:
            href = tag.find('a').get('href')
            cur = int(href.split('=')[-1])
            top = max(top, cur)
        except:
            continue

    return top

def refactor_name(s: str) -> str:
    return s.replace('\n', '').replace(' ', '').strip()

async def scrape_product(link: str, session) -> Product:
    today = date.today().strftime('%d/%m/%Y')
    attemps = 5
    while attemps:
        try:
            print(f'product {link} is scraping')
            proxy = random.choice(PROXIES)
            proxy_addres = proxy['proxy']
            proxy_auth = aiohttp.BasicAuth(proxy['user'], proxy['password'])
            resp = await session.get(url=link, proxy=proxy_addres, proxy_auth=proxy_auth)
            if resp.status == 200:
                soup = bs(await resp.text(), 'lxml')
                getTag = GetTagValue(soup)

                price_tag = soup.find('span', class_='item-price__num')
                price = price_tag.find('meta').get('content').replace(' ', '.').strip('.')

                name = refactor_name(getTag.scope('h1', {'class': 'js-with-nbsp-after-digit'}))

                tbody = soup.find('tbody')
                arr = []
                for td in tbody.find_all('td'):
                    arr.append(td.text.replace('\n', '').strip())
                brand_comp_dict = list_to_dict(arr)
                getDict = GetDictValue(brand_comp_dict)

                pics_tags = soup.find_all('img', class_='product_big_pic')
                pics = ''
                for pic in pics_tags:
                    pics += f'{BASE_DOMAIN}{pic.get("src")}, '

                desc = getTag.scope('p', {'itemprop': 'description'})

                old_price = getTag.oldprice()

                if old_price:
                    if float(old_price) > float(price):
                        return Product(
                            date=today,
                            brand=getDict.value('Бренд'),
                            name=name,
                            price=old_price,
                            promo_price=price,
                            card_price=price,
                            rating=None,
                            desc=desc,
                            comp=getDict.value('Состав'),
                            url=link,
                            pics=pics
                        )

                    return Product(
                        date=today,
                        brand=getDict.value('Бренд'),
                        name=name,
                        price=price,
                        promo_price=None,
                        card_price=None,
                        rating=None,
                        desc=desc,
                        comp=getDict.value('Состав'),
                        url=link,
                        pics=pics
                    )

                else:

                    return Product(
                        date=today,
                        brand=getDict.value('Бренд'),
                        name=name,
                        price=price,
                        promo_price=None,
                        card_price=None,
                        rating=None,
                        desc=desc,
                        comp=getDict.value('Состав'),
                        url=link,
                        pics=pics
                    )

            else:
                return Product(
                    date=today,
                    brand=resp.status,
                    name=resp.status,
                    price=resp.status,
                    promo_price=resp.status,
                    card_price=resp.status,
                    rating=resp.status,
                    desc=resp.status,
                    comp=resp.status,
                    url=link,
                    pics=resp.status
                )

        except Exception as e:
            attemps -= 1
            print(f'some exception at {link} -- {type(e).__name__}: {e}')

    return Product(
        date=today,
        brand='error',
        name='error',
        price='error',
        promo_price='error',
        card_price='error',
        rating='error',
        desc='error',
        comp='error',
        url=link,
        pics='error'
    )


async def scrape_page(page: str, session, file: TextIOWrapper) -> list[str]:
    print(f'page {page} is scraping')
    time.sleep(2)
    try:
        proxy = random.choice(PROXIES)
        proxy_addres = proxy['proxy']
        proxy_auth = aiohttp.BasicAuth(proxy['user'], proxy['password'])
        resp = await session.get(page, allow_redirects=True)
        soup = bs(await resp.text(), 'lxml')
        products = soup.find_all('div',
                                 class_='catalog-section__item__body trans')

        for product in products:
            link = product.find('a').get('href')
            print(link)
            file.write(f'{BASE_DOMAIN}{link}\n')
    except Exception as e:
        print(f'some exception during scraping links from exact page: -- {type(e).__name__}: {e} ')





async def scrape_links():
    with open('links.txt', 'w', encoding='utf-8') as file:
        async with CloudflareScraper(timeout=TIME_OUT) as session:
            response = await session.get(BASE_DOMAIN)
            soup = bs(await response.text(), 'lxml')
            categories_tags = soup.find('ul', class_='nav_main__content-list')
            categories_links = []
            for category in categories_tags:
                try:
                    link = category.find('a').get('href')
                    categories_links.append(BASE_DOMAIN+link)

                except Exception as e:
                    print(f'some exception during scraping categories links: -- {type(e).__name__}: {e} ')

            for category_link in categories_links:
                resp = await session.get(category_link)
                soup = bs(await resp.text(), 'lxml')
                pagination = soup.find('ul', class_='box-content box-shadow')
                max_page = find_last_page_number(soup=pagination)

                iterations = max_page // 5
                start = 1
                for i in range(iterations+1):
                    await asyncio.gather(
                        *(
                            scrape_page(page=f'{category_link}?PAGEN_1={cur_page}', session=session, file=file) for cur_page in range(start, i*5+1)
                        )
                    )
                    start = i * 5 + 1

                if max_page % 5:
                    await asyncio.gather(
                        *(
                            scrape_page(page=f'{category_link}?PAGEN_1={cur_page}', session=session, file=file) for cur_page in range(start, max_page+1)
                        )
                    )



async def create_json():
    links = open('links.txt').readlines()
    links = [link.replace('\n', '') for link in links]
    async with CloudflareScraper(timeout=TIME_OUT) as session:
        start = 1
        parts = 10
        iterations = len(links) // parts
        arr =  []
        for i in range(iterations+1):
            products = await asyncio.gather(
                *(
                    scrape_product(link=links[j], session=session) for j in range(start, i*parts+1)
                )
            )
            start = i*parts+1
            for product in products:
                arr.append(
                    {
                        'date': product.date,
                        'brand': product.brand,
                        'name': product.name,
                        'pics': product.pics,
                        'price': product.price,
                        'promo_price': product.promo_price,
                        'card_price': product.card_price,
                        'rating': product.rating,
                        'desc': product.desc,
                        'comp': product.comp,
                        'url': product.url
                    }
                )
            with open('globus.json', 'w', encoding='utf-8') as file:
                data = json.dumps(arr)
                data = json.loads(str(data))
                json.dump(data, file, indent=4, ensure_ascii=False)


        if len(links) % iterations:
            products = await asyncio.gather(
                *(
                    scrape_product(link=links[j], session=session) for j in range(start, len(links)+1)
                )
            )
            for product in products:
                arr.append(
                    {
                        'date': product.date,
                        'brand': product.brand,
                        'name': product.name,
                        'pics': product.pics,
                        'price': product.price,
                        'promo_price': product.promo_price,
                        'card_price': product.card_price,
                        'rating': product.rating,
                        'desc': product.desc,
                        'comp': product.comp,
                        'url': product.url
                    }
                )
            with open('globus.json', 'w', encoding='utf-8') as file:
                data = json.dumps(arr)
                data = json.loads(str(data))
                json.dump(data, file, indent=4, ensure_ascii=False)


asyncio.run(create_json())