import datetime

import pandas as pd

from source.creation_management.utils import CreationUtils


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

