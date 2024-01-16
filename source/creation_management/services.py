import datetime

import pandas as pd

from source.creation_management.utils import CreationUtils, make_head, make_tail


class CreationServices:

    def __init__(self):
        self.creation_utils = CreationUtils()

    async def prepare_to_creation_management(
            self, df: pd.DataFrame,
            products: list[dict],
            article_column: str,
            price_column: str,
            brand_name: str = None,
            prefix: str = None,
    ) -> list[str]:
        brand_name = '' if brand_name is None else f'{brand_name}_'
        products_df = pd.DataFrame([
            {'vendor_code': product['card'].get('vendor_code'), 'product': product}
            for product in products
        ])
        df[article_column] = df[article_column].apply(func=lambda item: str(item))

        if prefix is not None:
            products_df['vendor_code'] = products_df['vendor_code'].apply(func=lambda item: str(item).split(prefix)[-1])

        # final_df = pd.merge(df, products_df, how='inner', left_on=article_column, right_on='vendor_code')
        final_df = self.creation_utils.merge_products_and_return_df(
            df=df, products=products, article_column=article_column, price_column=price_column)

        print(final_df)
        print(final_df.columns)
        products = final_df.to_dict('records')
        output_products_df = pd.DataFrame(
            self.creation_utils.prepare_output_to_creation(products=products, price_column=price_column, vendor_code_column=article_column))

        products_filename = f'cached_files/{brand_name}products_to_be_created' \
                    + '_' + \
                    str(datetime.date.today()) + '.xlsx'

        output_products_df.to_excel(products_filename, index=False)

        excluded_df = self.creation_utils.excluded_df(
            initial_df=df, products_df=products_df, article_column=article_column)

        excluded_filename = f'cached_files/{brand_name}not_found_vendor_codes' \
                    + '_' + \
                    str(datetime.date.today()) + '.xlsx'

        excluded_df.to_excel(excluded_filename, index=False)
        return [excluded_filename, products_filename]

    async def prepare_to_creation_products_with_no_prices(self, products: list[dict]) -> pd.DataFrame:
        products = [
            {'product': product, 'price': product['detail'].get('priceU', 0) // 100}
            for product in products
        ]

        return pd.DataFrame(
            self.creation_utils.prepare_output_to_creation(products=products, price_column='price'))

    async def prepare_set_products(self, df: pd.DataFrame, major_nm_id_column, minor_nm_id_column, major_price_tail, minor_price_tail):
        major_products_df = pd.DataFrame([
            {'major_nm_id': product['card'].get('nm_id'), 'major_product': product}
            for product in await self.creation_utils.get_detail_by_nms(nms=list(df[major_nm_id_column]))
        ])
        minor_products_df = pd.DataFrame([
            {'minor_nm_id': product['card'].get('nm_id'), 'minor_product': product}
            for product in await self.creation_utils.get_detail_by_nms(nms=list(df[minor_nm_id_column]))
        ])

        df = df.merge(major_products_df, how='inner', left_on=major_nm_id_column, right_on='major_nm_id') \
            .merge(minor_products_df, how='inner', left_on=minor_nm_id_column, right_on='minor_nm_id')

        output_data = []
        for index in df.index:
            major_product: dict = df['major_product'][index]
            minor_product: dict = df['minor_product'][index]
            major_tail: int = df[major_price_tail][index]
            minor_tail: int = df[minor_price_tail][index]
            output_data.append(
                await self.creation_utils.prepare_set_product(
                    major_product=major_product, minor_product=minor_product,
                    major_tail=major_tail, minor_tail=minor_tail
                )
            )
        return output_data

    async def create_instantly_products(self, df: pd.DataFrame, token, nm_id_column: str):
        products = await self.creation_utils.get_detail_by_nms(nms=list(df[nm_id_column]))



