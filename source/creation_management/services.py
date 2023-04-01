import datetime

import pandas as pd

from source.creation_management.utils import CreationUtils


class CreationServices:

    def __init__(self):
        self.creation_utils = CreationUtils()

    async def prepare_to_creation_management(
            self, df: pd.DataFrame, products: list[dict], article_column: str, price_column: str) -> list[str]:
        products_df = pd.DataFrame([
            {'vendor_code': product['card'].get('vendor_code'), 'product': product}
            for product in products
        ])

        final_df = pd.merge(df, products_df, how='inner', left_on=article_column, right_on='vendor_code')
        products = final_df.to_dict('records')
        output_products_df = pd.DataFrame(
            self.creation_utils.prepare_output_to_creation(products=products, price_column=price_column))

        products_filename = 'cached_files/products_to_be_created' \
                    + '_' + \
                    str(datetime.date.today()) + '.xlsx'

        output_products_df = output_products_df.drop_duplicates(subset=['Артикул продавца'])
        output_products_df.to_excel(products_filename, index=False)

        excluded_df = self.creation_utils.excluded_df(
            initial_df=df, products_df=products_df, article_column=article_column)

        excluded_filename = 'cached_files/not_found_vendor_codes' \
                    + '_' + \
                    str(datetime.date.today()) + '.xlsx'

        excluded_df.to_excel(excluded_filename, index=False)
        return [excluded_filename, products_filename]