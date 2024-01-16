import asyncio
import aiohttp
import json
from decouple import config
import pandas as pd


class BaseUtils:

    @staticmethod
    async def make_get_request(url, headers, no_json=False):
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as session:
            async with session.get(url=url) as response:

                if response.status == 200:
                    return True if no_json else json.loads(await response.text())

    @staticmethod
    async def make_post_request(url, headers, payload, no_json=False):
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as session:
            async with session.post(url=url, json=payload) as response:
                print(response.status)

                if response.status == 200:
                    return True if no_json else json.loads(await response.text())


class GPTUtils(BaseUtils):
    @staticmethod
    def gpt_auth():
        return {'Authorization': f'Bearer {config("GPT_API_KEY")}'}

    async def make_simple_request(self, question: str) -> str:
        headers = self.gpt_auth()
        url = 'https://api.openai.com/v1/chat/completions'

        payload = {
            "model": "gpt-3.5-turbo",
            "messages": [{'role': 'user', 'content': question}],
        }
        data = await self.make_post_request(url=url, headers=headers, payload=payload)
        return data.get('choices', [{}])[0].get('message', {}).get('content')

    async def gpt_make_description(self, major_product, minor_product):
        promt = f"""Из двух текстов описаний товара сделай один текст описание с плавным переходои
        Текст 1: "{major_product['card'].get('description')}"
        Текст 2: "{minor_product['card'].get('description')}"
        """
        return await self.make_simple_request(question=promt)

    async def gpt_make_product_title(self, major_product, minor_product):
        promt = f"""Соедени два названия двух товаров в одно название товара. Максимум на 55 символов/букв
        Название 1: {major_product['card'].get('imt_name')}
        Название 2: {minor_product['card'].get('imt_name')}
        """
        return await self.make_simple_request(question=promt)


class CreationUtils(GPTUtils):

    async def get_catalog(self, url):
        products = []

        for page in range(1, 101):
            print(page, 'catalog page')
            page_url = url.format(page=page)
            data = await self.make_get_request(page_url, headers={})

            if data:
                data = data['data']['products']
                products += data
                if len(data) != 100:
                    break
        return products

    async def get_all_catalogs_from_brands(self, brand_ids):
        products = []
        for brand_id in brand_ids:
            url = 'https://catalog.wb.ru/brands/h/catalog?appType=1&brand=%s&couponsGeo=12,3,18,15,21&curr=rub&dest=-455203&emp=0&lang=ru&locale=ru&page={page}&pricemarginCoeff=1.0&reg=1&regions=80,64,38,4,83,33,68,70,69,30,86,75,40,1,66,31,48,110,22,71&sort=popular&spp=27&sppFixGeo=4' % brand_id
            print(brand_id)
            products += await self.get_catalog(url=url)
        return products

    async def get_product_data(self, article):
        card_url = make_head(int(article)) + make_tail(str(article), 'info/ru/card.json')
        obj = {}
        card = await self.make_get_request(url=card_url, headers={})
        detail_url = f'https://card.wb.ru/cards/detail?spp=27&regions=80,64,38,4,83,33,68,70,69,30,86,75,40,1,22,66,31,48,110,71&pricemarginCoeff=1.0&reg=1&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&sppFixGeo=4&dest=-455203&nm={article}'
        detail = await self.make_get_request(detail_url, headers={})

        if detail:
            detail = detail['data']['products']
        else:
            detail = {}

        qnt_url = f'https://product-order-qnt.wildberries.ru/by-nm/?nm={article}'
        qnt = await self.make_get_request(url=qnt_url, headers={})

        obj.update({
            'card': card if card else {},
            'detail': detail[0] if detail else {},
            'qnt': qnt if qnt else {}
            # 'seller': seller_data if seller_data else {}
        })

        return obj

    async def get_products(self, brand_ids):
        products = await self.get_all_catalogs_from_brands(brand_ids=brand_ids)
        output_data = []

        tasks = []
        count = 1

        for product in products:
            task = asyncio.create_task(self.get_product_data(article=product.get('id')))
            tasks.append(task)
            count += 1

            if count % 50 == 0:
                print(count, 'product data')
                output_data += await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

        output_data += await asyncio.gather(*tasks, return_exceptions=True)
        output_data = [item for item in output_data if not isinstance(item, Exception) and item]
        return output_data

    async def get_detail_by_nms(self, nms):
        output_data = []
        tasks = []

        for index, nm in enumerate(nms, start=1):
            task = asyncio.create_task(self.get_product_data(article=nm))
            tasks.append(task)

            if index % 50 == 0:
                print(index, 'product data')
                output_data += await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

        output_data += await asyncio.gather(*tasks, return_exceptions=True)
        output_data = [item for item in output_data if not isinstance(item, Exception) and item]
        return output_data

    async def get_by_seller_id(self, seller_id: int):
        url = 'https://catalog.wb.ru/sellers/catalog?curr=rub&dest=-1257786&page={page}&regions=80,64,38,4,115,83,33,68,70,69,30,86,75,40,1,66,48,110,22,31,71,114,111&sort=popular&spp=25&supplier=%s' % seller_id
        products = await self.get_catalog(url=url)

        output_data = []
        tasks = []
        count = 0
        for product in products:
            task = asyncio.create_task(self.get_product_data(article=product.get('id')))
            tasks.append(task)
            count += 1

            if count % 50 == 0:
                print(count, 'product data')
                output_data += await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

        output_data += await asyncio.gather(*tasks, return_exceptions=True)
        output_data = [item for item in output_data if not isinstance(item, Exception) and item]
        return output_data

    @staticmethod
    def prepare_output_to_creation(products: list[dict], price_column: str, vendor_code_column: str = None) -> list[dict]:
        output_data = []

        for item in products:
            product = item.get('product', {'card': {}, 'detail': {}})
            nm_id = product['detail'].get('id')
            if nm_id is None:
                continue

            price = item.get(price_column, 0)

            options_dict = dict()
            options = product['card'].get('options', [])

            for option in options:
                options_dict.update({option.get('name'): option.get('value').split(' ')[0]})

            media = product['card'].get('media', '')
            if media:
                photo_count = product['card']['media'].get('photo_count', 2)
                photo_count = 2 if photo_count == 1 else photo_count

                images_url = ';'.join([
                    make_head(int(nm_id)) + make_tail(str(nm_id), f'images/big/{image_count}.jpg')
                    for image_count in range(1, photo_count)
                ])
            else:
                images_url = None

            if vendor_code_column:
                vendor_code = 'bland' + item.get(vendor_code_column, '')
            else:
                vendor_code = 'bland' + product['card'].get('vendor_code', '')

            obj = {
                'Номер карточки': product['detail'].get('id'),
                'Предмет': product['card'].get('subj_name'),
                'subj_root_name': product['card'].get('subj_root_name'),
                'Цвет': None,
                'Бренд': product['detail'].get('brand'),
                'Пол': None,
                'Название': product['detail'].get('name', '')[:60],
                'Артикул продавца': vendor_code,
                'Баркод товара': None,
                'Цена': price,
                'Описание': product['card'].get('description'),
                'Медиафайлы': images_url,
            }
            obj.update(options_dict)
            output_data.append(obj)

        return output_data

    async def prepare_set_product(self, major_product, minor_product, major_tail, minor_tail) -> dict:
        major_nm_id, minor_nm_id = major_product['card'].get('nm_id'), minor_product['card'].get('nm_id')
        options_dict = dict()
        options = major_product['card'].get('options', [])

        for option in options:
            options_dict.update({option.get('name'): option.get('value').split(' ')[0]})

        major_image_urls = [
            make_head(int(major_nm_id)) + make_tail(str(major_nm_id), f'images/big/{count}.jpg')
            for count in range(1, major_product['card']['media'].get('photo_count', 1) + 1)
        ]
        minor_image_urls = [
            make_head(int(minor_nm_id)) + make_tail(str(minor_nm_id), f'images/big/{count}.jpg')
            for count in range(1, minor_product['card']['media'].get('photo_count', 1) + 1)
        ]

        imt_name = await self.gpt_make_product_title(major_product=major_product, minor_product=minor_product)
        description = await self.gpt_make_description(major_product=major_product, minor_product=minor_product)

        major_price = major_product['detail'].get('salePriceU', 0) // 100
        minor_price = minor_product['detail'].get('salePriceU', 0) // 100
        price = (major_price - (major_price * major_tail / 100)) + (minor_price - (minor_price * minor_tail / 100))

        vendor_code = f'bland{major_product["card"].get("vendor_code", "")}_|_bland{minor_product["card"].get("vendor_code", "")}'
        image_url = ';'.join(major_image_urls + minor_image_urls)
        return {
            'Номер карточки': f"{major_product['detail'].get('id')}_|_{minor_product['detail'].get('id')}",
            'Предмет': major_product['card'].get('subj_name'),
            'subj_root_name': major_product['card'].get('subj_root_name'),
            'Цвет': None,
            'Бренд': major_product['detail'].get('brand'),
            'Пол': None,
            'Название': imt_name,
            'Артикул продавца': vendor_code,
            'Баркод товара': None,
            'Цена': round(price),
            'Описание': description,
            'Медиафайлы': image_url,
            **options_dict
        }

    @staticmethod
    def excluded_df(initial_df: pd.DataFrame, products_df: pd.DataFrame, article_column: str):

        if initial_df.empty:
            return products_df

        if products_df.empty:
            return initial_df

        return pd.merge(
            initial_df, products_df,  left_on=article_column, right_on='vendor_code', how="outer", indicator=True)\
            .query('_merge=="left_only"')

    @staticmethod
    def sort_products_by_sales(products: list[dict]) -> list[dict]:
        products_dict = dict()
        output_data = []

        for product in products:
            vendorCode = product['card'].get('vendor_code')
            if products_dict.get(vendorCode):
                products_dict[vendorCode].append(product)
            else:
                products_dict[vendorCode] = [product]

        for key, product_list in products_dict.items():
            print([p.keys() for p in product_list])
            output_data.append(
                max(product_list, key=lambda item: item['qnt'][0].get('qnt', 0))
            )
        return output_data

    @staticmethod
    def merge_products_and_return_df(df: pd.DataFrame, products: list[dict],
                                     article_column: str, price_column: str) -> pd.DataFrame:
        output_data = []
        for index in df.index:
            vendor_code = df[article_column][index]
            for product in products:
                if vendor_code in product['card'].get('vendor_code', ''):
                    output_data.append({
                        'Артикул WB': vendor_code,
                        'Минимальная цена': df[price_column][index],
                        'vendor_code': product['card'].get('vendor_code', ''),
                        'product': product,
                    })
                    break
        return pd.DataFrame(output_data)


def make_head(article: int):
    head = 'https://basket-{i}.wb.ru'

    if article < 14400000:
        number = '01'
    elif article < 28800000:
        number = '02'
    elif article < 43500000:
        number = '03'
    elif article < 72000000:
        number = '04'
    elif article < 100800000:
        number = '05'
    elif article < 106300000:
        number = '06'
    elif article < 111600000:
        number = '07'
    elif article < 117000000:
        number = '08'
    elif article < 131400000:
        number = '09'
    elif article < 160200000:
        number = '10'
    elif article < 165600000:
        number = '11'
    else:
        number = '12'
    return head.format(i=number)


def make_tail(article: str, item: str):
    length = len(str(article))
    if length <= 3:
        return f'/vol{0}/part{0}/{article}/' + item
    elif length == 4:
        return f'/vol{0}/part{article[0]}/{article}/' + item
    elif length == 5:
        return f'/vol{0}/part{article[:2]}/{article}/' + item
    elif length == 6:
        return f'/vol{article[0]}/part{article[:3]}/{article}/' + item
    elif length == 7:
        return f'/vol{article[:2]}/part{article[:4]}/{article}/' + item
    elif length == 8:
        return f'/vol{article[:3]}/part{article[:5]}/{article}/' + item
    else:
        return f'/vol{article[:4]}/part{article[:6]}/{article}/' + item
