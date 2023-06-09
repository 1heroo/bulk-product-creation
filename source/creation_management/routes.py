import io

import pandas as pd
from fastapi import APIRouter, File
from starlette import status
from starlette.responses import JSONResponse, StreamingResponse

from source.creation_management.services import CreationServices
from source.creation_management.xlsx_utils import XlsxUtils

router = APIRouter(prefix='/kt-creation', tags=['KT creation'])

xlsx_utils = XlsxUtils()
creation_services = CreationServices()


@router.post('/create-cards-by-vendor-code/')
async def create_kts(brand_id: int, brand_name: str, file: bytes = File()):

    df = pd.read_excel(file)
    article_column = df['Артикул WB'].name
    price_column = df['Минимальная цена'].name

    if article_column is None:
        return JSONResponse(content={'message': 'Не правильная струкутра в экзель'},
                            status_code=status.HTTP_400_BAD_REQUEST)

    products = await creation_services.creation_utils.get_products(brand_ids=[brand_id])
    print(len(products))
    products = creation_services.creation_utils.sort_products_by_sales(products=products)
    filenames = await creation_services.prepare_to_creation_management(
        products=products,
        df=df, article_column=article_column, price_column=price_column, brand_name=brand_name)

    return xlsx_utils.zip_response(filenames=filenames, zip_filename='products.zip')


@router.post('/create-cards-by-seller-id/')
async def create_by_seller_id(seller_id: int, prefix_vendor_code: str = None, file: bytes = File()):

    df = pd.read_excel(file)

    article_column = df['Артикул WB'].name
    price_column = df['Минимальная цена'].name

    if article_column is None:
        return JSONResponse(content={'message': 'Не правильная струкутра в экзель'},
                            status_code=status.HTTP_400_BAD_REQUEST)

    products = await creation_services.creation_utils.get_by_seller_id(seller_id=seller_id)

    filenames = await creation_services.prepare_to_creation_management(
        products=products,
        df=df, article_column=article_column, price_column=price_column, prefix=prefix_vendor_code)

    return xlsx_utils.zip_response(filenames=filenames, zip_filename='products.zip')


@router.post('/get-products-by-articles-wb/')
async def get_products_by_articles_wb(file: bytes = File()):
    df = pd.read_excel(file).dropna()
    nm_id_column = df['Артикул WB'].name

    products = await creation_services.creation_utils.get_detail_by_nms(nms=list(df[nm_id_column]))
    for product in products:
        print(product.get('card').get('nm_id'))
    products_df = await creation_services.prepare_to_creation_products_with_no_prices(products=products)
    return xlsx_utils.streaming_response(df=products_df, file_name='products-by-articles-wb')


@router.get('/get-seller-products-by-seller-id/{seller_id}/')
async def get_seller_products_by_seller_id(seller_id: int):
    products = await creation_services.creation_utils.get_by_seller_id(seller_id=seller_id)
    products_df = await creation_services.prepare_to_creation_products_with_no_prices(products=products)

    return xlsx_utils.streaming_response(df=products_df, file_name=f'products seller {seller_id}')

