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
async def create_kts(brand_id: int, file: bytes = File()):

    df = pd.read_excel(file)
    article_column = df['Артикул WB'].name
    price_column = df['Минимальная цена'].name

    if article_column is None:
        return JSONResponse(content={'message': 'Не правильная струкутра в экзель'},
                            status_code=status.HTTP_400_BAD_REQUEST)

    products = await creation_services.creation_utils.get_products(brand_ids=[brand_id])

    filenames = await creation_services.prepare_to_creation_management(
        products=products,
        df=df, article_column=article_column, price_column=price_column)

    return xlsx_utils.zip_response(filenames=filenames, zip_filename='products.zip')


@router.post('/create-cards-by-seller-id/')
async def create_by_seller_id(seller_id: int, file: bytes = File()):

    df = pd.read_excel(file)

    article_column = df['Артикул WB'].name
    price_column = df['Минимальная цена'].name

    if article_column is None:
        return JSONResponse(content={'message': 'Не правильная струкутра в экзель'},
                            status_code=status.HTTP_400_BAD_REQUEST)

    products = await creation_services.creation_utils.get_by_seller_id(seller_id=seller_id)

    filenames = await creation_services.prepare_to_creation_management(
        products=products,
        df=df, article_column=article_column, price_column=price_column)

    return xlsx_utils.zip_response(filenames=filenames, zip_filename='products.zip')
