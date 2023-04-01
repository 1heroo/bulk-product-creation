import io
import time

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
    df = df.drop_duplicates(subset=[article_column])

    if article_column is None:
        return JSONResponse(content={'message': 'Не правильная струкутра в экзель'},
                            status_code=status.HTTP_400_BAD_REQUEST)

    filenames = await creation_services.prepare_to_creation_management(
        df=df, brand_id=brand_id, article_column=article_column, price_column=price_column)

    return xlsx_utils.zip_response(filenames=filenames, zip_filename='products.zip')
